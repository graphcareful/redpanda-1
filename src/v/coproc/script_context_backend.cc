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
    if (!success) {
        /// In the case crc checks failed, do NOT write records to storage
        co_return;
    }
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

static ss::future<> create_materialized_topic(
  model::topic_namespace source,
  model::topic_namespace new_materialized,
  bool is_leader,
  output_write_args args) {
    /// If the materialized topic alreday exists, do nothing
    if (args.cache.get_topic_cfg(new_materialized)) {
        return ss::now();
    }
    /// See if theres an associated shared promise, if not create one. It is
    /// resolved when the desired materialized topic has been created.
    auto [itr, _] = args.topic_promises.emplace(
      new_materialized, ss::shared_promise());
    if (!is_leader) {
        /// In either case if the source topic is not a leader do not let it
        /// attempt to make a create_topics request, wait until the topic has
        /// been created.
        return itr->second.get_shared_future();
    }

    auto item = args.cache.get_topic_cfg(source);
    /// TODO: Potential race between deletion of source topic and creation of
    /// materialized topic, for now not an issue as deletion of source topics is
    /// prohibited.
    vassert(
      item, "Source topic for materialized topic doesn't exist: {}", source);
    item->tp_ns.tp = new_materialized.tp;
    item->properties.source_topic = source.tp();
    return args.frontend.create_topics({std::move(*item)}, model::no_timeout)
      .then([&tps = args.topic_promises,
             new_materialized = std::move(new_materialized)](
              std::vector<cluster::topic_result> result) {
          vassert(
            result.size() == 1, "Expected only one topic_result response");
          const auto ec = result[0].ec;
          if (ec == cluster::errc::success) {
              vlog(
                coproclog.info,
                "Created materialized log: {}",
                new_materialized);
          } else if (ec == cluster::errc::topic_already_exists) {
              vlog(
                coproclog.info,
                "Materialzed log has come into existance via another node: {}",
                new_materialized);
          } else {
              return ss::make_exception_future<>(
                materialized_topic_replication_exception(fmt::format(
                  "Failed to replicate materialized topic: {}, error: {}",
                  new_materialized,
                  result[0].ec)));
          }
          /// Alert waiters and cleanup state used for notification mechanism.
          auto found = tps.find(new_materialized);
          found->second.set_value();
          tps.erase(found);
          return ss::now();
      });
}

static ss::future<storage::log>
get_log(storage::log_manager& log_mgr, const model::ntp& ntp) {
    auto found = log_mgr.get(ntp);
    if (found) {
        /// Log exists, do nothing and return it
        return ss::make_ready_future<storage::log>(*found);
    }
    /// In the case the storage::log has not been created, but the topic has,
    /// register to recieve a notification when it is created.
    ss::promise<storage::log> p;
    auto f = p.get_future();
    auto id = log_mgr.register_manage_notification(
      ntp, [p = std::move(p)](storage::log log) mutable { p.set_value(log); });
    return f.then([&log_mgr, id](storage::log log) {
        log_mgr.unregister_manage_notification(id);
        return log;
    });
}

static ss::future<> write_materialized_partition(
  const model::ntp& ntp,
  model::record_batch_reader reader,
  ss::lw_shared_ptr<ntp_context> ctx,
  output_write_args args) {
    auto found = args.locks.find(m_ntp.input_ntp());
    if (found == args.locks.end()) {
        found = args.locks.emplace(m_ntp.input_ntp(), mutex()).first;
    }
    return found->second.with(
      [ctx, ntp, args, reader = std::move(reader)]() mutable {
          return create_materialized_topic(
                   model::topic_namespace(ctx->ntp().ns, ctx->ntp().tp.topic),
                   model::topic_namespace(ntp.ns, ntp.tp.topic),
                   ctx->partition->is_leader(),
                   args)
            .then([args, ntp] { return get_log(args.log_manager, ntp); })
            .then([ntp, reader = std::move(reader)](storage::log log) mutable {
                return do_write_materialized_partition(log, std::move(reader));
            });
      });
}

/// TODO: If we group replies by destination topic, we can increase write
/// throughput, be attentive to maintain relative ordering though..
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
    try {
        co_await write_materialized_partition(
          e.ntp, std::move(*e.reader), ntp_ctx, args);
    } catch (const crc_failed_exception& ex) {
        vlog(
          coproclog.error,
          "Reprocessing record, failure encountered, {}",
          ex.what());
        co_return;
    } catch (const materialized_topic_replication_exception& ex) {
        vlog(
          coproclog.error,
          "Failed to create materialized topic: {}",
          ex.what());
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
