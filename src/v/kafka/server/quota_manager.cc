// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "kafka/server/quota_manager.h"

#include "config/configuration.h"
#include "kafka/server/logger.h"
#include "vlog.h"

#include <fmt/chrono.h>

#include <chrono>

namespace kafka {
using clock = quota_manager::clock;
using throttle_delay = quota_manager::throttle_delay;

quota_manager::~quota_manager() { _gc_timer.cancel(); }

ss::future<> quota_manager::stop() {
    _gc_timer.cancel();
    return ss::make_ready_future<>();
}

ss::future<> quota_manager::start() {
    _gc_timer.arm_periodic(_gc_freq);
    return ss::make_ready_future<>();
}

quota_manager::underlying_t::iterator
quota_manager::maybe_add_and_retrieve_quota(
  const std::optional<std::string_view>& client_id,
  const clock::time_point& now) {
    // requests without a client id are grouped into an anonymous group that
    // shares a default quota. the anonymous group is keyed on empty string.
    auto cid = client_id ? *client_id : "";

    // find or create the throughput tracker for this client
    //
    // c++20: heterogeneous lookup for unordered_map can avoid creation of
    // an sstring here but std::unordered_map::find isn't using an
    // equal_to<> overload. this is a general issue we'll be looking at. for
    // now, these client-name strings are small. This will be solved in
    // c++20 via Hash::transparent_key_equal.
    auto [it, inserted] = _quotas.try_emplace(
      ss::sstring(cid),
      quota{
        now,
        clock::duration(0),
        {static_cast<size_t>(_default_num_windows()), _default_window_width()},
        {*_target_partition_mutation_quota(),
         static_cast<uint32_t>(_default_num_windows()),
         _default_window_width()}});

    // bump to prevent gc
    if (!inserted) {
        it->second.last_seen = now;
    }

    return it;
}

// record new desired number of partition mutations
void quota_manager::record_partition_mutations(
  std::optional<std::string_view> client_id,
  uint32_t mutations,
  clock::time_point now) {
    auto it = maybe_add_and_retrieve_quota(client_id, now);
    it->second.pm_rate.record(mutations, now);
}

// record a new observation and return <previous delay, new delay>
throttle_delay quota_manager::record_tp_and_throttle(
  std::optional<std::string_view> client_id,
  uint64_t bytes,
  bool partition_mutation_request,
  clock::time_point now) {
    auto it = maybe_add_and_retrieve_quota(client_id, now);

    auto rate = it->second.tp_rate.record_and_measure(bytes, now);
    bool partition_quota_exceeded = false;

    std::chrono::milliseconds delay_ms(0);
    if (rate > _target_tp_rate()) {
        auto diff = rate - _target_tp_rate();
        double delay = (diff / _target_tp_rate())
                       * static_cast<double>(
                         std::chrono::duration_cast<std::chrono::milliseconds>(
                           it->second.tp_rate.window_size())
                           .count());
        delay_ms = std::chrono::milliseconds(static_cast<uint64_t>(delay));
    }

    /// KIP-599 throttles create_topics / delete_topics / create_partitions
    /// request. This delay should only be applied to these requests if the
    /// quota has been exceeded
    if (partition_mutation_request && _target_partition_mutation_quota()) {
        const auto units = it->second.pm_rate.measure(now);
        if (units < 0) {
            /// Throttle time is defined as -K * R, where K is the number of
            /// tokens in the bucket and R is the avg rate. This only works when
            /// the number of tokens are negative which is the case when the
            /// rate limiter recommends throttling
            const auto rate = (units * -1)
                              * std::chrono::seconds(
                                *_target_partition_mutation_quota());
            const auto delay_pm_ms
              = std::chrono::duration_cast<std::chrono::milliseconds>(rate);
            if (delay_pm_ms > delay_ms) {
                delay_ms = delay_pm_ms;
                partition_quota_exceeded = true;
            }
        }
    }

    std::chrono::milliseconds max_delay_ms(_max_delay());
    if (delay_ms > max_delay_ms) {
        vlog(
          klog.info,
          "Found data rate for window of: {} bytes. Client:{}, Estimated "
          "backpressure delay of {}. Limiting to {} backpressure delay",
          rate,
          it->first,
          delay_ms,
          max_delay_ms);
        delay_ms = max_delay_ms;
    }

    auto prev = it->second.delay;
    it->second.delay = delay_ms;

    throttle_delay res{};
    res.first_violation = prev.count() == 0;
    res.duration = it->second.delay;
    res.partition_quota_exceeded = partition_quota_exceeded;
    return res;
}
// erase inactive tracked quotas. windows are considered inactive if they
// have not received any updates in ten window's worth of time.
void quota_manager::gc(clock::duration full_window) {
    auto now = clock::now();
    auto expire_age = full_window * 10;
    // c++20: replace with std::erase_if
    absl::erase_if(
      _quotas, [now, expire_age](const std::pair<ss::sstring, quota>& q) {
          return (now - q.second.last_seen) > expire_age;
      });
}

} // namespace kafka
