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

#include "coproc/logger.h"
#include "coproc/pacemaker.h"
#include "storage/log_manager.h"
#include "vlog.h"

#include <seastar/core/coroutine.hh>

namespace coproc {

reconciliation_backend::reconciliation_backend(
  model::node_id self,
  storage::log_manager& log_manager,
  cluster::topic_table& topics,
  wasm::script_database& sdb,
  ss::sharded<pacemaker>& pacemaker) noexcept
  : _self(self)
  , _log_manager(log_manager)
  , _topics(topics)
  , _sdb(sdb)
  , _pacemaker(pacemaker) {}

ss::future<> reconciliation_backend::start() {
    (void)ss::with_gate(_gate, [this] {
        return ss::do_until(
          [this] { return _as.abort_requested(); },
          [this] {
              return fetch_deltas().handle_exception(
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

static std::optional<topic_ingestion_policy> policy_for_topic(
  const std::vector<topic_namespace_policy>& topics,
  model::topic_namespace_view tn) {
    /// Yes this is an O(n) scan but we will never expect there to be a
    /// huge amount of input topics for a single coprocessor, also this only
    /// runs on partition movement
    auto found = std::find_if(
      topics.begin(), topics.end(), [tn](const topic_namespace_policy& tnp) {
          return tnp.tn == tn;
      });
    return found == topics.end()
             ? std::nullopt
             : std::optional<topic_ingestion_policy>(found->policy);
}

ss::future<> reconciliation_backend::do_process_script_restarts(
  absl::node_hash_set<script_id> ids,
  model::ntp ntp,
  ss::shard_id target_shard) {
    absl::node_hash_map<script_id, topic_ingestion_policy> tip_map;
    for (script_id id : ids) {
        const auto cp_find = _sdb.find(id);
        vassert(
          cp_find != _sdb.cend(), "Inconsistency detected in script_database");
        auto op = policy_for_topic(
          cp_find->second.inputs, model::topic_namespace_view{ntp});
        vassert(op.has_value(), "Inconsistency detected in script database");
        tip_map[id] = *op;
    }
    /// Assemble all state to be invoked on 'target_shard' instead of calling
    /// '.invoke_on' within a loop
    return _pacemaker.invoke_on(
      target_shard,
      [ntp, tip_map = std::move(tip_map)](pacemaker& p) -> ss::future<> {
          /// TODO: Error handling
          for (const auto [id, policy] : tip_map) {
              co_await p.restart_partition(id, policy, ntp);
          };
      });
}

ss::future<> reconciliation_backend::do_process_restarts(finishes_t finishes) {
    for (const auto& item : finishes) {
        const auto& ntp = item.first;
        const auto& assignments = item.second;
        for (const auto& assignment : assignments.replicas) {
            if (_self != assignment.node_id) {
                continue;
            }
            const auto found = _sdb.find(
              model::topic_namespace{ntp.ns, ntp.tp.topic});
            if (found == _sdb.inv_cend()) {
                vlog(
                  coproclog.error,
                  "Failed to migrate partition {}, script missing from "
                  "database",
                  ntp);
                co_return;
            }
            co_await do_process_script_restarts(
              found->second, ntp, assignment.shard);
        }
    }
}

ss::future<> reconciliation_backend::process_shutdowns(moves_t to_shutdown) {
    std::vector<ss::future<>> fs;
    for (const model::ntp& ntp : to_shutdown) {
        auto errc = co_await _pacemaker.local().shutdown_partition(ntp);
        vassert(
          errc != errc::partition_has_pending_update,
          "Synchronization issue with copro partition movement");
        auto materialized_partitions = _topics.materialized_children(ntp);
        fs.reserve(materialized_partitions.size());
        for (const auto& partition : materialized_partitions) {
            fs.push_back(_log_manager.remove(partition));
        }
    }
    co_await ss::when_all_succeed(fs.begin(), fs.end());
}

ss::future<> reconciliation_backend::process_restarts(finishes_t to_restart) {
    if (to_restart.empty()) {
        return ss::now();
    }
    return container().invoke_on(
      copro_reconciliation_shard,
      [to_restart = std::move(to_restart)](reconciliation_backend& r) mutable {
          return r.do_process_restarts(std::move(to_restart));
      });
}

ss::future<> reconciliation_backend::fetch_deltas() {
    return _topics.wait_for_changes(_as).then([this](deltas_t deltas) {
        return ss::with_semaphore(
          _topics_sem, 1, [this, deltas = std::move(deltas)]() mutable {
              moves_t to_shutdown;
              finishes_t to_restart;
              for (auto& d : deltas) {
                  if (d.type == event_type::update) {
                      to_shutdown.push_back(d.ntp);
                  } else if (d.type == event_type::update_finished) {
                      to_restart.emplace(d.ntp, d.new_assignment);
                  }
              }
              return process_shutdowns(std::move(to_shutdown))
                .then([this, restarts = std::move(to_restart)]() mutable {
                    return process_restarts(std::move(restarts));
                });
          });
    });
}

} // namespace coproc
