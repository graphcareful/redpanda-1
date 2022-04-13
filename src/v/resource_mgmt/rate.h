/*
 * Copyright 2020 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once
#include "likely.h"
#include "seastarx.h"

#include <seastar/core/lowres_clock.hh>
#include <seastar/util/bool_class.hh>

#include <chrono>
#include <numeric>
#include <vector>

// rate_tracker tracks the rate of a metric over time using a sliding window
// average. configure with number of windows and the width of each window.
//
// this class effectively merges Kafka's SampledStat and Rate classes together
// so that the same computation is done with a single pass over the samples for
// the common case of recording a new value and sampling the rate.
class rate_tracker final {
public:
    using clock = ss::lowres_clock;

private:
    struct window {
        clock::time_point time;
        double count{0.};
        window() = default;
    };

public:
    rate_tracker(size_t num_windows, clock::duration window_size)
      : _windows(num_windows)
      , _window_size(window_size)
      , _current(0) {}

    // record an observation and return the updated rate in units/second.
    double record_and_measure(double v, const clock::time_point& now) {
        // update the current window
        maybe_advance_current(now);
        _windows[_current].count += v;

        // process historical samples
        double total = 0.;
        struct window* oldest = nullptr;
        auto max_age = _window_size * _windows.size();
        for (auto& w : _windows) {
            // purge windows that are obsolete
            if ((now - w.time) > max_age) {
                w.time = now;
                w.count = 0;
            }

            // find oldest non-obsolete window
            if (!oldest || w.time < oldest->time) {
                oldest = &w;
            }

            // accumulate
            total += w.count;
        }

        // compute window size and current rate. the reason that the elapsed
        // time is adjusted up is to error conservatively to allow utilization
        // to ramp up until a steady state is hit in which all the windows are
        // fully used. this is an identical approach taken by the kafka broker.
        auto elapsed = now - oldest->time;
        auto num_windows = elapsed / _window_size;
        auto min_windows = _windows.size() - 1;
        // chrono uses signed integers
        if ((size_t)num_windows < min_windows) {
            elapsed += (min_windows - num_windows) * _window_size;
        }

        return total / std::chrono::duration<double>(elapsed).count();
    }

    clock::duration window_size() const { return _window_size; }

private:
    // maybe advance to and reset the next window
    void maybe_advance_current(clock::time_point now) {
        if ((_windows[_current].time + _window_size) < now) {
            _current = (_current + 1) % _windows.size();
            auto& curr = _windows[_current];
            curr.time = now;
            curr.count = 0.;
        }
    }

    std::vector<window> _windows;
    clock::duration _window_size;
    std::size_t _current;
};

/// Token bucket algorithm, useful for rate-limiting solutions.
///
/// Keeps track of a limited number of resources known as 'tokens'.
/// The current amount of tokens are kept in \ref bucket and when a user
/// consumes resources they take a corresponding number of tokens from the
/// bucket. Every \ref window duration the pre-configured amount of units are
/// added to the bucket. If there are enough tokens in the bucket when queried
/// the algorithm recommends rate-limiting, otherwise not.
class token_bucket_rate_tracker {
public:
    using clock = ss::lowres_clock;
    using units_t = int64_t;
    using recommend_rate_limit
      = ss::bool_class<struct recommend_rate_limit_tag>;

    /// Class constructor
    /// @param quota Expected rate to operate within
    /// @param samples Number of windows to accumulate in memory for burst
    /// @param window_sec width of window in which tokens are added - in seconds
    token_bucket_rate_tracker(
      uint32_t quota, uint32_t samples, clock::duration window_sec) noexcept
      : _quota(quota)
      , _samples(samples)
      , _window(std::chrono::duration_cast<std::chrono::seconds>(window_sec))
      , _last_check() {}

    /// Slight variation of token bucket algorithm to allow bursty traffic
    /// \ref burst tokens are removed from bucket after returning throttle
    /// behavior suggestion
    recommend_rate_limit
    record_and_measure(uint32_t n, const clock::time_point& now) noexcept {
        refill(now);
        if (_bucket < 0) {
            return recommend_rate_limit::yes;
        }
        _bucket = std::min<units_t>(burst(), _bucket - n);
        return recommend_rate_limit::no;
    }

    void record(uint32_t n, const clock::time_point& now) noexcept {
        refill(now);
        if (_bucket >= 0) {
            /// Number of tokens in bucket is allowed to become negative to
            /// allow burst of traffic to proceed
            _bucket = std::min<units_t>(burst(), _bucket - n);
        }
    }

    units_t measure(const clock::time_point& now) noexcept {
        refill(now);
        return _bucket;
    }

    /// Returns the number of tokens that current remain in the bucket
    units_t units() const noexcept { return _bucket; }

private:
    void refill(const clock::time_point& now_ms) noexcept {
        const auto delta = std::chrono::duration_cast<std::chrono::seconds>(
          now_ms - _last_check);
        _last_check = now_ms;
        _bucket = std::min<units_t>(burst(), _bucket + delta.count() * _quota);
    }

    uint32_t burst() const noexcept {
        constexpr uint32_t max_val = std::numeric_limits<uint32_t>::max();
        const auto b64 = static_cast<uint64_t>(_quota)
                         * static_cast<uint64_t>(_samples)
                         * static_cast<uint64_t>(_window.count());
        return unlikely(b64 >= max_val) ? max_val : b64;
    }

private:
    uint32_t _quota;
    uint32_t _samples;
    units_t _bucket{0};
    std::chrono::seconds _window;
    clock::time_point _last_check;
};
