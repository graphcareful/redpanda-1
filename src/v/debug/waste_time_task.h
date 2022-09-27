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

#include "debug/logger.h"
#include "debug/types.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/loop.hh>

#include <chrono>
#include <exception>

using namespace std::chrono_literals;

namespace debug {

class waste_time_task_already_scheduled : public std::exception {};

class waste_time_task {
public:
    ss::future<test_results> start(ss::lowres_clock::time_point stoptime) {
        return ss::with_gate(_gate, [this, stoptime] {
            return ss::do_until(
                     [stoptime] { return stoptime <= ss::lowres_clock::now(); },
                     [this] {
                         if (_as.abort_requested()) {
                             vlog(dbg.info, "Abort requested exiting ... ");
                             return ss::now();
                         }
                         vlog(dbg.info, "Iterating... ");
                         return ss::sleep_abortable(1s, _as)
                           .handle_exception_type([](ss::sleep_aborted) {
                               vlog(dbg.info, "Aborted while sleeping ... ");
                           });
                     })
              .then([] {
                  vlog(dbg.info, "Task complete");
                  return test_results();
              });
        });
    }

    ss::future<> stop() {
        _as.request_abort();
        return _gate.close();
    }

private:
    ss::gate _gate;
    ss::abort_source _as;
};

class disk_test : public waste_time_task {};
class network_test : public waste_time_task {};

} // namespace debug
