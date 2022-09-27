/*
 * Copyright 2022 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "debug/service.h"

#include "debug/logger.h"

namespace debug {

service::service(ss::scheduling_group sc, ss::smp_service_group ssg)
  : self_test_service(sc, ssg) {}

ss::future<std::vector<test_results>>
service::do_start(test_parameters params) {
    auto r1 = co_await _disk_test.start(
      ss::lowres_clock::now() + params.disk_test_timeout_sec);
    auto r2 = co_await _network_test.start(
      ss::lowres_clock::now() + params.network_test_timeout_sec);
    co_return std::vector<test_results>{std::move(r1), std::move(r2)};
}

ss::future<self_test_start_reply>
service::start(self_test_start_request&& req, rpc::streaming_context&) {
    if (are_jobs_running()) {
        co_return self_test_start_reply{
          .id = _last_test, .current_job = status::in_progress};
    }
    const auto new_id = req.id;
    (void)ss::with_gate(_gate, [this, req = std::move(req)]() mutable {
        _last_test = req.id;
        _disk_test = disk_test();
        _network_test = network_test();
        return do_start(req.parameters)
          .then([this](std::vector<test_results> results) {
              _previous_results = std::move(results);
          })
          .handle_exception([this](std::exception_ptr eptr) {
              try {
                  std::rethrow_exception(eptr);
              } catch (const std::exception& ex) {
                  vlog(
                    dbg.info,
                    "Exception ocurred while running self-test: {}",
                    ex);
                  _previous_results = {
                    test_results({{"exception", ex.what()}})};
              }
          });
    });
    co_return self_test_start_reply{
      .id = new_id, .current_job = status::started};
}

ss::future<self_test_stop_reply>
service::stop(self_test_stop_request&& req, rpc::streaming_context&) {
    try {
        std::vector<ss::future<>> stopping;
        stopping.push_back(_disk_test.stop());
        stopping.push_back(_network_test.stop());
        co_await ss::when_all_succeed(stopping.begin(), stopping.end());
    } catch (const std::exception& ex) {
        vlog(
          dbg.info,
          "Exception occurred while canceling self-test jobs: {}",
          ex);
    }
    co_return self_test_stop_reply{};
}

ss::future<self_test_status_reply>
service::status(self_test_status_request&& req, rpc::streaming_context&) {
    co_return self_test_status_reply{.id = _last_test};
}

} // namespace debug
