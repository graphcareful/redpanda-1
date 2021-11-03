/*
 * Copyright 2021 Vectorized, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "script_context_backend.h"

#include "cluster/metadata_cache.h"
#include "cluster/non_replicable_topics_frontend.h"
#include "coproc/exception.h"
#include "coproc/logger.h"
#include "coproc/reference_window_consumer.hpp"
#include "storage/api.h"
#include "storage/log.h"
#include "storage/log_manager.h"
#include "storage/parser_utils.h"
#include "vlog.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/sleep.hh>

namespace coproc {
class follower_create_topic_exception final : public exception {
    using exception::exception;
};

class log_not_yet_created_exception final : public exception {
    using exception::exception;
};

class bad_reply_exception final : public exception {
    using exception::exception;
};

/// Sets all of the term_ids in a batch of record batches to be a newly
/// desired term.
class set_term_id_to_zero {
public:
    ss::future<ss::stop_iteration> operator()(model::record_batch& b) {
        b.header().ctx.term = model::term_id(0);
        _batches.push_back(std::move(b));
        co_return ss::stop_iteration::no;
    }

    model::record_batch_reader end_of_stream() {
        return model::make_memory_record_batch_reader(std::move(_batches));
    }

private:
    model::record_batch_reader::data_t _batches;
};

static ss::future<> do_write_materialized_partition(
  storage::log log, model::record_batch_reader reader) {
    /// Re-write all batch term_ids to 1, otherwise they will carry the
    /// term ids of records coming from parent batches
    auto [success, batch_w_correct_terms]
      = co_await std::move(reader).for_each_ref(
        reference_window_consumer(
          model::record_batch_crc_checker(), set_term_id_to_zero()),
        model::no_timeout);

    vassert(
      success,
      "Batch failed crc checks, check wasm engine impl: {}",
      log.config().ntp());

    /// Compress the data before writing...
    auto compressed = co_await std::move(batch_w_correct_terms)
                        .for_each_ref(
                          storage::internal::compress_batch_consumer(
                            model::compression::zstd, 512),
                          model::no_timeout);
    const storage::log_append_config write_cfg{
      .should_fsync = storage::log_append_config::fsync::no,
      .io_priority = ss::default_priority_class(),
      .timeout = model::no_timeout};
    /// Finally, write the batch
    co_await std::move(compressed)
      .for_each_ref(log.make_appender(write_cfg), model::no_timeout)
      .discard_result();
}

static ss::future<storage::log>
get_log(storage::log_manager& log_mgr, model::ntp ntp) {
    auto found = log_mgr.get(ntp);
    if (found) {
        /// Log exists, do nothing and return it
        co_return *found;
    }
    /// In the case the storage::log has not been created, but the topic has.
    throw log_not_yet_created_exception(fmt::format(
      "Materialized topic {} created but underlying log doesn't yet exist",
      ntp));
}

static ss::future<> maybe_make_materialized_log(
  model::topic_namespace source,
  model::topic_namespace new_materialized,
  bool is_leader,
  output_write_args args) {
    const auto& cache = args.rs.metadata_cache.local();
    if (cache.get_topic_cfg(new_materialized)) {
        if (cache.get_source_topic(new_materialized)) {
            /// Log already exists
            co_return;
        }
        /// Attempt to produce onto a normal topic..., shutdown
        throw script_illegal_action_exception(
          args.id,
          fmt::format(
            "Script {} attempted to write to a normal topic: {}",
            args.id,
            new_materialized));
    }
    /// Leader could be on a different machine, can only wait until log comes
    /// into existance
    if (!is_leader) {
        throw follower_create_topic_exception(fmt::format(
          "Follower of source topic {} attempted to created materialzied "
          "topic {} before leader partition had a chance to, sleeping "
          "1s...",
          source,
          new_materialized));
    }
    /// Create new materialized topic
    cluster::non_replicable_topic mt{
      .source = std::move(source), .name = std::move(new_materialized)};
    std::vector<cluster::non_replicable_topic> topics{std::move(mt)};
    /// All requests are debounced, therefore if multiple entities attempt to
    /// create a materialzied topic, all requests will wait for the first to
    /// complete.
    co_return co_await args.rs.mt_frontend.invoke_on(
      cluster::non_replicable_topics_frontend_shard,
      [topics = std::move(topics)](
        cluster::non_replicable_topics_frontend& mtfe) mutable {
          return mtfe.create_non_replicable_topics(
            std::move(topics), model::no_timeout);
      });
}

static ss::future<> write_materialized_partition(
  const model::ntp& ntp,
  model::record_batch_reader reader,
  ss::lw_shared_ptr<cluster::partition> input,
  output_write_args args) {
    /// For the rational of why theres mutex uses here read relevent comments in
    /// coproc/shared_script_resources.h
    auto found = args.locks.find(ntp);
    if (found == args.locks.end()) {
        found = args.locks.emplace(ntp, mutex()).first;
    }
    return found->second.with(
      [args, ntp, input, reader = std::move(reader)]() mutable {
          model::topic_namespace source(input->ntp().ns, input->ntp().tp.topic);
          model::topic_namespace new_materialized(ntp.ns, ntp.tp.topic);
          return maybe_make_materialized_log(
                   source, new_materialized, input->is_leader(), args)
            .then([args, ntp] {
                return get_log(args.rs.storage.local().log_mgr(), ntp);
            })
            .then([reader = std::move(reader)](storage::log log) mutable {
                return do_write_materialized_partition(log, std::move(reader));
            });
      });
}

static ss::future<>
process_one_reply(process_batch_reply::data e, output_write_args args) {
    /// Ensure this 'script_context' instance is handling the correct reply
    if (e.id != args.id()) {
        /// TODO: Maybe in the future errors of these type should mean redpanda
        /// kill -9's the wasm engine.
        throw bad_reply_exception(fmt::format(
          "erranous reply from wasm engine, mismatched id observed, expected: "
          "{} and observed {}",
          args.id,
          e.id));
    }
    if (!e.reader) {
        throw script_failed_exception(
          e.id,
          fmt::format(
            "script id {} will auto deregister due to an internal syntax "
            "error",
            e.id));
    }
    auto found = args.inputs.find(e.source);
    if (found == args.inputs.end()) {
        throw bad_reply_exception(
          fmt::format("script {} unknown source ntp: {}", args.id, e.source));
    }
    auto src = found->second;
    auto [cur, _] = src->wctx.offsets.try_emplace(
      e.ntp, src->rctx.absolute_start);
    /// Only write to partitions for which are up to date with the current
    /// in progress read. Upon success bump offset to next batches start
    if (cur->second == src->rctx.last_acked) {
        co_await write_materialized_partition(
          e.ntp, std::move(*e.reader), src->rctx.input, args);
        src->wctx.offsets[e.ntp] = src->rctx.last_read;
    }
}

ss::future<>
write_materialized(output_write_inputs replies, output_write_args args) {
    if (replies.empty()) {
        vlog(
          coproclog.error, "Wasm engine interpreted the request as erraneous");
        co_return;
    }
    /// Dispatch all replies in parallel. No need to worry about multiple
    /// replies from same apply call that write to the same destination topic as
    /// wasm engine groups these replies into one
    bool err{false};
    co_await ss::parallel_for_each(
      replies,
      [args, &err](output_write_inputs::value_type& e) -> ss::future<> {
          try {
              co_await process_one_reply(std::move(e), args);
          } catch (const bad_reply_exception& ex) {
              vlog(coproclog.error, "Erraneous response detected: {}", ex);
          } catch (const script_failed_exception& ex) {
              throw ex;
          } catch (const coproc::exception& ex) {
              /// For any type of failure the offset will not be touched,
              /// the read phase will always read from the global min of all
              /// offsets ever registered.
              vlog(coproclog.info, "Error while processing reply: {}", ex);
              err = true;
          }
      });
    if (err) {
        co_await ss::sleep(100ms);
    }
}

} // namespace coproc
