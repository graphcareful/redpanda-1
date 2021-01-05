/*
 * Copyright 2020 Vectorized, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "coproc/script_context.h"

#include "coproc/logger.h"
#include "coproc/pacemaker_context.h"
#include "coproc/types.h"
#include "coproc/utils.h"
#include "model/record_batch_reader.h"
#include "model/timeout_clock.h"
#include "vlog.h"

namespace coproc {

script_context::script_context(
  script_id id, pacemaker_context& ctx, ntp_context_cache&& contexts)
  : scheduled_task(std::chrono::milliseconds(20))
  , _ctx(ctx)
  , _ntp_ctxs(std::move(contexts))
  , _id(id) {
    vassert(
      !_ntp_ctxs.empty(),
      "Unallowed to create an instance of script_context without having a "
      "valid subscription list");
}

ss::future<> script_context::shutdown() {
    _abort_source.request_abort();
    return scheduled_task::shutdown().then([this] { _ntp_ctxs.clear(); });
}

ss::future<> script_context::do_task() {
    std::vector<process_batch_request::data> requests;
    requests.reserve(_ntp_ctxs.size());
    return ss::do_with(
      std::move(requests),
      [this](std::vector<process_batch_request::data>& requests) {
          return ss::parallel_for_each(
                   _ntp_ctxs,
                   [this, &requests](const ntp_context_cache::value_type& p) {
                       return read_ntp(p.second).then(
                         [&requests](
                           std::optional<process_batch_request::data> r) {
                             if (r) {
                                 requests.push_back(std::move(*r));
                             }
                         });
                   })
            .then([this, &requests] {
                if (requests.empty()) {
                    return ss::now();
                }
                return _ctx
                  .send_request(
                    process_batch_request{.reqs = std::move(requests)})
                  .then_wrapped(
                    [this](ss::future<pacemaker_context::rpc_result> f) {
                        return process_reply(std::move(f));
                    });
            });
      });
}

std::optional<storage::log_reader_config>
script_context::get_reader(const ss::lw_shared_ptr<ntp_context>& ntp_ctx) {
    auto found = ntp_ctx->offsets.find(_id);
    vassert(
      found != ntp_ctx->offsets.end(),
      "script_id must exist: {} for ntp: {}",
      _id,
      ntp_ctx->ntp);
    const model::offset last_read = found->second;
    auto stats = ntp_ctx->log.offsets();
    if (last_read >= stats.dirty_offset()) {
        return std::nullopt;
    }
    return reader_cfg(
      last_read + model::offset(1), _max_batch_size, _abort_source);
}

ss::future<std::optional<process_batch_request::data>>
script_context::read_ntp(ss::lw_shared_ptr<ntp_context> ntp_ctx) {
    return ss::with_semaphore(_ctx.read_sem(), 1, [this, ntp_ctx]() {
        auto cfg = get_reader(ntp_ctx);
        if (!cfg) {
            return ss::make_ready_future<
              std::optional<process_batch_request::data>>(std::nullopt);
        }
        return ntp_ctx->log.make_reader(*cfg).then(
          [this, ntp_ctx](model::record_batch_reader rbr) {
              return extract_batch_info(std::move(rbr))
                .then(
                  [this, ntp_ctx](std::optional<batch_info> obatch_info)
                    -> std::optional<process_batch_request::data> {
                      if (!obatch_info) {
                          return std::nullopt;
                      }
                      ntp_ctx->offsets[_id] = obatch_info->last;
                      return process_batch_request::data{
                        .ids = std::vector<script_id>{_id},
                        .ntp = ntp_ctx->ntp,
                        .reader = std::move(obatch_info->rbr)};
                  });
          });
    });
}

ss::future<>
script_context::process_reply(ss::future<pacemaker_context::rpc_result> f) {
    try {
        auto reply = f.get0();
        if (reply) {
            return ss::do_with(
              std::move(reply.value().data),
              [this](process_batch_reply& reply) {
                  return ss::do_for_each(
                    reply.resps, [this](process_batch_reply::data& e) {
                        return process_one_reply(std::move(e));
                    });
              });
        }
        vlog(coproclog.error, "Error on copro request: {}", reply.error());
    } catch (const std::exception& e) {
        vlog(coproclog.error, "Copro request future threw: {}", e.what());
    }
    return ss::now();
}

ss::future<> script_context::process_one_reply(process_batch_reply::data e) {
    /// Ensure this 'script_context' instance is handling the correct reply
    if (e.id != _id) {
        return ss::make_exception_future<>(std::logic_error(fmt::format(
          "id: {} recieved response from another script_id: {}", _id, e.id)));
    }
    /// Use the source topic portion of the materialized topic to perform a
    /// lookup for the relevent 'ntp_context'
    auto materialized_ntp = model::materialized_ntp(e.ntp);
    auto found = _ntp_ctxs.find(materialized_ntp.source_ntp());
    if (found == _ntp_ctxs.end()) {
        vlog(
          coproclog.warn,
          "script {} unknown source ntp: {}",
          _id,
          materialized_ntp.source_ntp());
        return ss::now();
    }
    auto& ntp_ctx = found->second;
    auto ofound = ntp_ctx->offsets.find(_id);
    vassert(
      ofound != ntp_ctx->offsets.end(),
      "Missing offset for script id {} for ntp owning context: {}",
      _id,
      found->first);
    return _ctx.write_materialized(materialized_ntp, std::move(e.reader))
      .then([](bool crc_parse_failure) {
          if (crc_parse_failure) {
              vlog(coproclog.warn, "record_batch failed to pass crc checks");
          }
          /// TODO(rob) durably save offset
      });
}

} // namespace coproc
