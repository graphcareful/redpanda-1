/*
 * Copyright 2021 Vectorized, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "coproc/reconciliation_backend.h"

#include "cluster/topic_table.h"
#include "coproc/logger.h"
#include "coproc/pacemaker.h"

#include <seastar/core/coroutine.hh>

namespace coproc {

reconciliation_backend::reconciliation_backend(
  ss::sharded<cluster::topic_table>& topics,
  ss::sharded<cluster::shard_table>& shard_table,
  ss::sharded<storage::api>& storage,
  ss::sharded<pacemaker>& pacemaker) noexcept
  : _topics(topics)
  , _shard_table(shard_table)
  , _storage(storage)
  , _pacemaker(pacemaker) {}

ss::future<> reconciliation_backend::start() {
    (void)ss::with_gate(_gate, [this] {
        return ss::do_until(
          [this] { return _as.abort_requested(); },
          [this] {
              return fetch_and_reconcile().handle_exception(
                [](const std::exception_ptr& e) {
                    vlog(
                      coproclog.error,
                      "Error while reconciling topics - {}",
                      e);
                });
          });
    });
    return ss::now();
}

ss::future<> reconciliation_backend::stop() {
    _as.request_abort();
    return _gate.close();
}

ss::future<> reconciliation_backend::fetch_and_reconcile() {
    auto deltas = co_await _topics.local().wait_for_non_rep_changes(_as);
    using delta_grouping_t
      = absl::flat_hash_map<model::ntp, std::vector<update_t>>;
    delta_grouping_t topic_deltas;
    for (auto& d : deltas) {
        auto ntp = d.ntp;
        topic_deltas[ntp].push_back(std::move(d));
    }

    co_await ss::parallel_for_each(
      topic_deltas.begin(),
      topic_deltas.end(),
      [this](delta_grouping_t::value_type& deltas) {
          return process_update(deltas.second);
      });
}

ss::future<>
reconciliation_backend::process_updates(std::vector<update_t> deltas) {
    auto hold = _gate.hold();
    auto fn = [this, deltas = std::move(deltas)]() mutable -> ss::future<> {
        for (auto& d : deltas) {
            co_await process_update(std::move(d));
        }
    };
    co_await ss::with_semaphore(_sem, 1, std::move(fn));
}

ss::future<> reconciliation_backend::process_update(update_t) {}

} // namespace coproc
