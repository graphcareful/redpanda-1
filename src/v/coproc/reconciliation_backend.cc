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
  ss::sharded<storage::api>& storage,
  ss::sharded<pacemaker>& pacemaker) noexcept
  : _data_directory(config::node().data_directory().as_sstring())
  , _topics(topics)
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

ss::future<> reconciliation_backend::process_update(update_t update) {
    using op_t = update_t::op_type;
    switch(update.type){
    case op_t::add:
        if (!has_local_replicas(_self, delta.new_assignment.replicas)) {
            return ss::make_ready_future<std::error_code>(errc::success);
        }
        return create_non_replicable_partition(delta.ntp, rev);
    case op_t::del:
        return delete_non_replicable_partition(delta.ntp, rev).then([] {
            return std::error_code(errc::success);
        });
    }
}

ss::future<> reconciliation_backend::delete_non_replicable_partition(
  model::ntp ntp, model::revision_id rev) {
    vlog(coproclog.trace, "removing {} from shard table at {}", ntp, rev);
    co_await _shard_table.invoke_on_all(
      [ntp, rev](cluster::shard_table& st) { st.erase(ntp, rev); });
    auto log = _storage.local().log_mgr().get(ntp);
    if (log && log->config().get_revision() < rev) {
        co_await _storage.local().log_mgr().remove(ntp);
    }
}

ss::future<std::error_code>
reconciliation_backend::create_non_replicable_partition(
  model::ntp ntp, model::revision_id rev) {
    auto cfg = _topics.local().get_topic_cfg(model::topic_namespace_view(ntp));
    if (!cfg) {
        // partition was already removed, do nothing
        co_return errc::success;
    }
    vassert(
      !_storage.local().log_mgr().get(ntp),
      "Log exists for missing entry in topics_table");
    auto ntp_cfg = cfg->make_ntp_config(_data_directory, ntp.tp.partition, rev);
    co_await _storage.local().log_mgr().manage(std::move(ntp_cfg));
    co_await add_to_shard_table(std::move(ntp), ss::this_shard_id(), rev);
    co_return errc::success;
}

ss::future<> reconciliation_backend::add_to_shard_table(
  model::ntp ntp, ss::shard_id shard, model::revision_id revision) {
    vlog(
      coproclog.trace, "adding {} to shard table at {}", revision, ntp, shard);
    return _shard_table.invoke_on_all(
      [ntp = std::move(ntp), shard, revision](cluster::shard_table& s) mutable {
          vassert(
            s.update_shard(ntp, shard, revision),
            "Newer revision for non-replicable ntp exists");
      });
}

} // namespace coproc
