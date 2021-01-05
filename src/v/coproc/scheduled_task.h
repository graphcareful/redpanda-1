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

#pragma once
#include "coproc/logger.h"
#include "random/simple_time_jitter.h"
#include "vassert.h"
#include "vlog.h"

#include <seastar/core/gate.hh>
#include <seastar/core/timer.hh>
#include <seastar/util/later.hh>

#include <chrono>
namespace coproc {

class scheduled_task {
public:
    explicit scheduled_task(std::chrono::milliseconds ms)
      : _jitter(ms) {
        _timer.set_callback([this] { (void)execute_task(); });
    }

    virtual ss::future<> start() {
        if (!_gate.is_closed()) {
            _timer.arm(_jitter());
        }
        return ss::now();
    }

    virtual ss::future<> do_task() = 0;

    virtual ss::future<> shutdown() {
        _timer.cancel();
        return _gate.close();
    }

private:
    using jitter_time_point = simple_time_jitter<ss::lowres_clock>::time_point;

    ss::future<> execute_task() {
        try {
            return do_execute_task().then(
              [this](std::pair<jitter_time_point, bool> next) {
                  /// These values are passed to this future because it is not
                  /// allowed to access member variables outside of the gate.
                  /// Outside of the gate, the gate could be closed which means
                  /// an external entity has waited on the future returned to by
                  /// 'shutdown()' and is free to destroy 'this'. Use after free
                  /// has been observed if this is not implemeneted
                  auto [jitter, gate_is_closed] = next;
                  if (!gate_is_closed) {
                      _timer.arm(jitter);
                  }
              });
        } catch (ss::gate_closed_exception& gce) {
            vlog(
              coproclog.debug,
              "Gate closed exception encountered, method: {}",
              gce);
        }
        return ss::now();
    }

    ss::future<std::pair<jitter_time_point, bool>> do_execute_task() {
        return ss::with_gate(_gate, [this] {
            vassert(_gate.get_count() == 1, "Double do_task() detected");
            return do_task().then_wrapped([this](ss::future<> f) {
                if (f.failed()) {
                    vlog(
                      coproclog.warn,
                      "do_task() threw, continuing loop: {}",
                      f.get_exception());
                }
                return std::make_pair(_jitter(), _gate.is_closed());
            });
        });
    }

private:
    ss::gate _gate;

    simple_time_jitter<ss::lowres_clock> _jitter;
    ss::timer<ss::lowres_clock> _timer;
};

} // namespace coproc
