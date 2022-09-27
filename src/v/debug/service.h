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

#pragma once
#include "debug/self_test_gen_service.h"
#include "debug/waste_time_task.h"
#include "seastarx.h"

#include <seastar/core/scheduling.hh>
namespace debug {

/// self_test orchestration service runs on every node, peers will blindly
/// follow commands issued by the test orchestrator running on the controller
/// node. Some of the commands are to start a test, stop a test, and query for
/// test status/results
class service final : public self_test_service {
public:
    service(ss::scheduling_group sc, ss::smp_service_group ssg);

    ss::future<self_test_start_reply>
    start(self_test_start_request&& req, rpc::streaming_context&) final;

    ss::future<self_test_stop_reply>
    stop(self_test_stop_request&& req, rpc::streaming_context&) final;

    ss::future<self_test_status_reply>
    status(self_test_status_request&& req, rpc::streaming_context&) final;

private:
    bool are_jobs_running() const { return _gate.get_count() > 0; }

    ss::future<std::vector<test_results>> do_start(test_parameters params);

private:
    ss::gate _gate;
    disk_test _disk_test;
    network_test _network_test;

    test_id _last_test{-1};
    std::vector<test_results> _previous_results;
};

} // namespace debug
