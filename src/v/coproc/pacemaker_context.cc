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

#include "coproc/pacemaker_context.h"

#include "coproc/logger.h"
#include "coproc/supervisor.h"
#include "coproc/utils.h"
#include "vlog.h"
namespace coproc {
pacemaker_context::pacemaker_context(
  ss::sharded<storage::api>& api, rpc::reconnect_transport& transport)
  : _api(api)
  , _transport(transport) {}

ss::future<result<rpc::client_context<process_batch_reply>>>
pacemaker_context::send_request(process_batch_request r) {
    return get_client().then([this, r = std::move(r)](auto transport) mutable {
        return ss::with_semaphore(
          _send_sem,
          1,
          [r = std::move(r), transport = std::move(transport)]() mutable {
              return transport.value().process_batch(
                std::move(r), rpc::client_opts(model::no_timeout));
          });
    });
}

ss::future<bool> pacemaker_context::write_materialized(
  const model::materialized_ntp& m_ntp, model::record_batch_reader reader) {
    return get_log(m_ntp.input_ntp())
      .then([reader = std::move(reader)](storage::log log) mutable {
          return write_checked(std::move(log), std::move(reader));
      });
}

ss::future<result<supervisor_client_protocol>> pacemaker_context::get_client() {
    using result_t = ::result<supervisor_client_protocol>;
    return _transport.get_connected().then(
      [this](result<rpc::transport*> transport) {
          if (!transport) {
              auto err = transport.error();
              if (err != rpc::errc::exponential_backoff) {
                  if (_connection_attempts++ == 5) {
                      return ss::make_ready_future<result_t>(
                        rpc::errc::disconnected_endpoint);
                  }
              }
              auto wait = _transport.next_expected_backoff_duration();
              vlog(
                coproclog.warn,
                "Failed attempt to connect to coproc server, attempt "
                "number: {}, sleeping for {}ms",
                _connection_attempts,
                wait.count());
              return ss::sleep(wait).then([this] { return get_client(); });
          }
          _connection_attempts = 0;
          return ss::make_ready_future<result_t>(
            coproc::supervisor_client_protocol(*transport.value()));
      });
}

ss::future<storage::log> pacemaker_context::get_log(const model::ntp& ntp) {
    auto found = _api.local().log_mgr().get(ntp);
    if (found) {
        return ss::make_ready_future<storage::log>(*found);
    }
    vlog(coproclog.info, "Making new log: {}", ntp);
    return _api.local().log_mgr().manage(
      storage::ntp_config(ntp, _api.local().log_mgr().config().base_dir));
}

} // namespace coproc
