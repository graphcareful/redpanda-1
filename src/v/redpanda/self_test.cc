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

#include "redpanda/self_test.h"

#include "json/types.h"
#include "seastarx.h"
#include "vlog.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/loop.hh>
#include <seastar/util/log.hh>

static ss::logger stlog{"self_test"};

namespace self_test {

ss::future<> harness::start() { return ss::now(); }

ss::future<> harness::stop() { co_await stop_test(); }

ss::future<ss::sstring> harness::do_start_test(test_parameters params) {
    /// Start tests
    std::vector<ss::future<stub_result>> started;
    vlog(stlog.info, "Starting disk self test...");
    started.push_back(
      _disk_test.start(ss::lowres_clock::now() + params.disk_test_timeout_sec)
        .finally([] { vlog(stlog.info, "...disk self test finished"); }));
    vlog(stlog.info, "Starting network self test...");
    started.push_back(
      _network_test
        .start(ss::lowres_clock::now() + params.network_test_timeout_sec)
        .finally([] { vlog(stlog.info, "...network self test finished"); }));

    /// When they finish, aggregate the results into one json object
    json::StringBuffer sb;
    json::Writer<json::StringBuffer> w(sb);
    w.StartObject();
    try {
        auto results = co_await ss::when_all_succeed(
          started.begin(), started.end());
        w.Key("disk_test_results");
        json::rjson_serialize(w, results[0]);
        w.Key("network_test_results");
        json::rjson_serialize(w, results[1]);
    } catch (const std::exception& ex) {
        vlog(stlog.error, "Self test errored with exception: {}", ex.what());
        w.Key("Error");
        json::rjson_serialize(w, "error_code::failed");
    }
    co_return sb.GetString();
}

status_report harness::start_test(uuid_t id, test_parameters parameters) {
    auto units = _m.try_get_units();
    if (!units) {
        return status_report{.ec = error_code::running};
    }
    _id = id;
    _disk_test = disk_test();
    _network_test = network_test();
    (void)do_start_test(parameters)
      .then([this](ss::sstring r) { _prev_run = std::move(r); })
      .finally([units = std::move(units)] {});
    return status_report{.id = id, .ec = error_code::success};
}

ss::future<status_report> harness::stop_test() {
    if (_m.ready()) {
        co_return status_report{.ec = error_code::stopped};
    }
    try {
        std::vector<ss::future<>> stopping;
        stopping.push_back(_disk_test.stop());
        stopping.push_back(_network_test.stop());
        co_await ss::when_all_succeed(stopping.begin(), stopping.end());
        co_return status_report{.id = _id, .ec = error_code::success};
    } catch (const std::exception& ex) {
        vlog(
          stlog.error,
          "Failed to successfully cancel all outstanding jobs: {}",
          ex.what());
    }
    co_return status_report{.id = _id, .ec = error_code::failed};
}

status_report harness::get_test_status() const {
    return status_report{
      .id = _id,
      .ec = (_m.ready() ? error_code::stopped : error_code::running)};
}

ss::sstring harness::get_last_results(uuid_t id) const {
    if (_id != id) {
        return "{\"error\": 1}";
    } else if (!_m.ready()) {
        return "{\"error\": 2}";
    }
    return _prev_run;
}

} // namespace self_test
