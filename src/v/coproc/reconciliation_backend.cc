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

#include "cluster/cluster_utils.h"
#include "cluster/shard_table.h"
#include "cluster/topic_table.h"
#include "coproc/logger.h"
#include "coproc/pacemaker.h"
#include "storage/api.h"

#include <seastar/core/coroutine.hh>

namespace coproc {

reconciliation_backend::reconciliation_backend(
  ss::sharded<cluster::topic_table>& topics,
  ss::sharded<cluster::shard_table>& shard_table,
  ss::sharded<storage::api>& storage) noexcept
  : _self(model::node_id(config::node().node_id))
  , _data_directory(config::node().data_directory().as_sstring())
  , _topics(topics)
  , _shard_table(shard_table)
  , _storage(storage) {}

ss::future<> reconciliation_backend::start() {
    _id_cb = _topics.local().register_delta_notification(
      [this](std::vector<update_t> deltas) {
          (void)ss::with_semaphore(
            _sem, 1, [this, deltas = std::move(deltas)]() mutable {
                for (auto& d : deltas) {
                    auto ntp = d.ntp;
                    _topic_deltas[ntp].push_back(std::move(d));
                }
                return fetch_and_reconcile();
            });
      });
    return ss::now();
}

ss::future<> reconciliation_backend::stop() {
    _as.request_abort();
    _topics.local().unregister_delta_notification(_id_cb);
    return _gate.close();
}

ss::future<> reconciliation_backend::process_events_for_ntp(
  model::ntp ntp, std::vector<update_t> updates) {
    for (auto& d : updates) {
        auto err = co_await process_update(d);
        if (err != errc::success) {
            vlog(
              coproclog.warn,
              "Result {} returned after processing update {}",
              err,
              ntp);
        }
    }
}

ss::future<> reconciliation_backend::fetch_and_reconcile() {
    auto hold = _gate.hold();
    if (_as.abort_requested()) {
        co_return;
    }
    auto deltas = std::exchange(_topic_deltas, {});
    co_await ss::parallel_for_each(
      deltas.begin(),
      deltas.end(),
      [this](decltype(_topic_deltas)::value_type& p) {
          return process_events_for_ntp(p.first, p.second);
      });
    if (!_topic_deltas.empty()) {
        vlog(
          coproclog.warn,
          "There were recoverable errors when processing events, retrying");
        co_await ss::sleep(100ms);
        co_await fetch_and_reconcile();
    }
}

ss::future<std::error_code> reconciliation_backend::process_update(update_t) {
    /// Empty as this commit is just about setting up the framework that will
    /// handle events. Commits that acutally handle the events are in subsequent
    /// commits within this patch series
    co_return errc::success;
}

} // namespace coproc
