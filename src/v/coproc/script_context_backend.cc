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

#include "coproc/logger.h"
#include "coproc/reference_window_consumer.hpp"
#include "storage/parser_utils.h"
#include "vlog.h"

#include <seastar/core/coroutine.hh>

namespace coproc {

class crc_failed_exception final : public std::exception {
public:
    explicit crc_failed_exception(ss::sstring msg) noexcept
      : _msg(std::move(msg)) {}

    const char* what() const noexcept final { return _msg.c_str(); }

private:
    ss::sstring _msg;
};

class materialized_topic_replication_exception : public std::exception {
public:
    explicit materialized_topic_replication_exception(ss::sstring msg) noexcept
      : _msg(std::move(msg)) {}

    const char* what() const noexcept final { return _msg.c_str(); }

private:
    ss::sstring _msg;
};

/// Sets all of the term_ids in a batch of record batches to be a newly
/// desired term.
class term_id_updater {
public:
    explicit term_id_updater(model::term_id tid)
      : _tid(tid) {}

    ss::future<ss::stop_iteration> operator()(model::record_batch& rb) {
        auto nh = rb.header();
        nh.ctx.term = _tid;
        _batches.push_back(
          model::record_batch(nh, std::move(rb).release_data()));
        co_return ss::stop_iteration::no;
    }

    model::record_batch_reader end_of_stream() {
        return model::make_memory_record_batch_reader(std::move(_batches));
    }

private:
    model::term_id _tid{};
    model::record_batch_reader::data_t _batches;
};

static ss::future<> do_write_materialized_partition(
  storage::log log,
  model::term_id highest_term,
  model::record_batch_reader reader) {
    /// Set all of the term_ids in each record to match the term of the source
    /// topic. Blindly copying the term that exists is incorrect as it could
    /// cause storage to crash in the event a coprocessor reorders
    /// record_batches potentially making a batch with a lower term appear after
    /// one with a larger term.
    model::term_id materialized_term = log.offsets().dirty_offset_term;
    /// In the case two coprocessors are writing to the same topics a
    /// lower term may be observed depending on where exactly in the input
    /// log each coprocessor is. To avoid this if detected, just use the
    /// current materialized logs term
    model::term_id new_term = highest_term < materialized_term
                                ? materialized_term
                                : highest_term;
    auto [crc_success, rbr] = co_await std::move(reader).for_each_ref(
      coproc::reference_window_consumer(
        model::record_batch_crc_checker(), term_id_updater(new_term)),
      model::no_timeout);
    if (!crc_success) {
        throw crc_failed_exception("A record batch failed to pass crc checks");
    }
    auto nrbr = co_await std::move(rbr).for_each_ref(
      storage::internal::compress_batch_consumer(model::compression::zstd, 512),
      model::no_timeout);

    const storage::log_append_config write_cfg{
      .should_fsync = storage::log_append_config::fsync::no,
      .io_priority = ss::default_priority_class(),
      .timeout = model::no_timeout};
    co_await std::move(nrbr).for_each_ref(
      log.make_appender(write_cfg), model::no_timeout);
}

static ss::future<> create_materialized_topic(
  cluster::topics_frontend& frontend,
  cluster::metadata_cache& cache,
  const model::topic_namespace_view& source,
  const model::topic& new_topic) {
    auto item = cache.get_topic_cfg(source);
    /// TODO: Potential race between deletion of source topic and creation of
    /// materialized topic, implement fix when we support deletion of source
    /// topics
    vassert(
      item, "Source topic for materialized topic doesn't exist: {}", source);
    item->tp_ns.tp = new_topic;
    item->properties.source_topic = source.tp();
    return frontend.create_topics({std::move(*item)}, model::no_timeout)
      .then([new_topic](std::vector<cluster::topic_result> result) {
          vassert(
            result.size() == 1, "Expected only one topic_result response");
          if (result[0].ec != cluster::errc::success) {
              throw materialized_topic_replication_exception(fmt::format(
                "Failed to replicate materialized topic: {}, error: {}",
                new_topic,
                result[0].ec));
          }
      });
}

static ss::future<storage::log> get_log(
  cluster::topics_frontend& frontend,
  cluster::metadata_cache& cache,
  storage::log_manager& log_mgr,
  model::ntp ntp,
  ss::lw_shared_ptr<ntp_context> ctx) {
    auto found = log_mgr.get(ntp);
    if (found) {
        /// Log exists, do nothing and return it
        return ss::make_ready_future<storage::log>(*found);
    }
    /// Topic itself doesn't exist
    if (!ctx->partition->is_leader()) {
        throw materialized_topic_replication_exception(
          "Must wait for leader to create topic before follower can create a "
          "log");
    }
    /// Leader of source topic requesting to create materialized topic
    return create_materialized_topic(
             frontend,
             cache,
             model::topic_namespace_view(ctx->ntp()),
             ntp.tp.topic)
      .then([&log_mgr, ntp] {
          /// The future returned by frontend::create_topic returns after the
          /// command has been replicated, this doesn't mean that the effects of
          /// the command (i.e. inserting to shardtable/storage::manage) have
          /// yet occurred
          auto log = log_mgr.get(ntp);
          if (log) {
              /// If underlying log has been created by the time this future is
              /// called
              vlog(coproclog.info, "Created materialized log: {}", ntp);
              return ss::make_ready_future<storage::log>(*log);
          }
          /// In the case the underlying log has not been created, register to
          /// recieve a notification when it is created
          ss::promise<storage::log> p;
          auto f = p.get_future();
          log_mgr.register_manage_notification(
            ntp, [ntp, p = std::move(p)](storage::log log) mutable {
                /// NOTE: This callback cannot be called twice, since the run
                /// loop of a coprocessor is single threaded progress won't be
                /// made until this future is resolved.
                /// NOTE: Other 'run_loops' via other coprocessors attempting to
                /// create the same materialized log will not be an issue since
                /// this method is accessed via a mutual exclusion lock
                vlog(coproclog.info, "Created materialized log: {}", ntp);
                p.set_value(log);
            });
          return f;
      });
}

static ss::future<> write_materialized_partition(
  model::ntp ntp,
  model::term_id highest_term,
  model::record_batch_reader reader,
  ss::lw_shared_ptr<ntp_context> ctx,
  output_write_args args) {
    auto found = args.locks.find(ntp);
    if (found == args.locks.end()) {
        found = args.locks.emplace(ntp, mutex()).first;
    }
    /// NOTE: Converting this to a coroutine causes a guaranteed crash even with
    /// clang.. why?
    return found->second.with(
      [ctx, ntp, highest_term, args, reader = std::move(reader)]() mutable {
          return get_log(args.frontend, args.cache, args.log_manager, ntp, ctx)
            .then([highest_term, ntp, reader = std::move(reader)](
                    storage::log log) mutable {
                return do_write_materialized_partition(
                  std::move(log), highest_term, std::move(reader));
            });
      });
}

static ss::future<>
process_one_reply(process_batch_reply::data e, output_write_args args) {
    /// Ensure this 'script_context' instance is handling the correct reply
    if (e.id != args.id()) {
        /// TODO: Maybe in the future errors of these type should mean redpanda
        /// kill -9's the wasm engine.
        vlog(
          coproclog.error,
          "erranous reply from wasm engine, mismatched id observed, expected: "
          "{} and observed {}",
          args.id,
          e.id);
        co_return;
    }
    if (!e.reader) {
        throw script_failed_exception(
          e.id,
          fmt::format(
            "script id {} will auto deregister due to an internal syntax "
            "error",
            e.id));
    }
    /// Use the source topic portion of the materialized topic to perform a
    /// lookup for the relevent 'ntp_context'
    auto found = args.inputs.find(e.source);
    if (found == args.inputs.end()) {
        vlog(
          coproclog.warn, "script {} unknown source ntp: {}", args.id, e.ntp);
        co_return;
    }
    auto ntp_ctx = found->second;
    model::term_id highest_term = ntp_ctx->partition->term();
    try {
        co_await write_materialized_partition(
          e.ntp, highest_term, std::move(*e.reader), ntp_ctx, args);
    } catch (const crc_failed_exception& ex) {
        vlog(
          coproclog.error,
          "Reprocessing record, failure encountered, {}",
          ex.what());
        co_return;
    } catch (const materialized_topic_replication_exception& ex) {
        vlog(
          coproclog.error, "Materialized topic already created {}", ex.what());
        co_return;
    }
    auto ofound = ntp_ctx->offsets.find(args.id);
    vassert(
      ofound != ntp_ctx->offsets.end(),
      "Offset not found for script id {} for ntp owning context: {}",
      args.id,
      ntp_ctx->ntp());
    /// Reset the acked offset so that progress can be made
    ofound->second.last_acked = ofound->second.last_read;
}

ss::future<>
write_materialized(output_write_inputs replies, output_write_args args) {
    if (replies.empty()) {
        vlog(
          coproclog.error, "Wasm engine interpreted the request as erraneous");
    } else {
        for (auto& e : replies) {
            co_await process_one_reply(std::move(e), args);
        }
    }
}

} // namespace coproc
