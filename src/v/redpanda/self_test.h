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
#include "json/document.h"
#include "self_tests/stub_test.h"
#include "utils/mutex.h"
#include "utils/uuid.h"

#include <chrono>

namespace self_test {

/// Self tests only run on one shard
static constexpr ss::shard_id main_test_shard = ss::shard_id(0);

enum class error_code { success, failed, running, stopped };

struct status_report {
    uuid_t id;
    error_code ec;
};

struct test_parameters {
    std::chrono::seconds disk_test_timeout_sec;
    std::chrono::seconds network_test_timeout_sec;
};

/// Class representing the logic and state of how redpanda self tests run
///
/// Within this class there is a mutex that ensures only one test can
/// be executing at once. start_test() immediately returns and tests execute in
/// the background, its the clients job to periodicially query for finished
/// tests. The last successful test results and its test identifier are cached
/// for later retrival.
class harness {
public:
    /// A noop, just provided as this will be called via ss::sharded<>::start()
    ss::future<> start();

    /// Called when stop is called on the service, just calls ::stop_test()
    ss::future<> stop();

    /// Starts a test if one isn't already executing
    /// Important to note that this method immediately returns, however it
    /// starts asynchronous jobs that will run in the background
    ///
    /// @param id the new id of the test to run
    /// @param parameters test parameters to be forwarded to the self tests
    /// @returns a success error_code if started successfully, errors in all
    ///   other cases
    status_report start_test(uuid_t id, test_parameters parameters);

    /// Stops all currently executing tests
    /// Important to note that the future returned will be resolved when all
    /// asynchronous work has stopped.
    ///
    /// @returns Future reporting final status after all self tests have been
    /// confirmed stopped
    ss::future<status_report> stop_test();

    /// Returns the current status of the system, i.e. are any tests currently
    /// running or not
    status_report get_test_status() const;

    /// Returns the last results for a given idenfitifer. Only the most recent
    /// completed runs results are cached, all older results will be discarded
    ///
    /// @param id Identifier of the test run to observe
    /// @returns Json representation of the results if they are ready and match
    ///   the given identifier, otherwise the json payload will contain an error
    ///   field
    ss::sstring get_last_results(uuid_t id) const;

private:
    ss::future<ss::sstring> do_start_test(test_parameters params);

private:
    // cached values
    uuid_t _id;
    ss::sstring _prev_run;

    mutex _m;
    disk_test _disk_test;
    network_test _network_test;
};

} // namespace self_test
