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
#include "cluster/partition_manager.h"
#include "cluster/shard_table.h"
#include "cluster/topic_table.h"
#include "coproc/logger.h"
#include "coproc/pacemaker.h"
#include "coproc/partition_manager.h"
#include "storage/api.h"

#include <seastar/core/coroutine.hh>

namespace coproc {

reconciliation_backend::reconciliation_backend(
  ss::sharded<cluster::topic_table>& topics,
  ss::sharded<cluster::shard_table>& shard_table,
  ss::sharded<cluster::partition_manager>& pm,
  ss::sharded<partition_manager>& partition_manager) noexcept
  : _self(model::node_id(config::node().node_id))
  , _data_directory(config::node().data_directory().as_sstring())
  , _topics(topics)
  , _shard_table(shard_table)
  , _pm(pm)
  , _partition_manager(partition_manager) {}

ss::future<> reconciliation_backend::start() {
    _id_cb = _topics.local().register_delta_notification(
      [this](std::vector<update_t> deltas) {
          for (auto& d : deltas) {
              if (d.is_non_replicable()) {
                  vlog(coproclog.info, "EVENT");
                  auto ntp = d.ntp;
                  _topic_deltas[ntp].push_back(std::move(d));
              }
          }
          if (!_as.abort_requested() && !_topic_deltas.empty()) {
              (void)ss::with_semaphore(_sem, 1, [this]() {
                  return ss::with_gate(
                    _gate, [this]() { return fetch_and_reconcile(); });
              });
          }
      });
    return ss::now();
}

ss::future<> reconciliation_backend::stop() {
    _as.request_abort();
    _topics.local().unregister_delta_notification(_id_cb);
    return _gate.close();
}

ss::future<> reconciliation_backend::fetch_and_reconcile() {
    auto deltas = std::exchange(_topic_deltas, {});
    co_await ss::parallel_for_each(
      deltas.begin(),
      deltas.end(),
      [this](decltype(_topic_deltas)::value_type& deltas) -> ss::future<> {
          for (auto& d : deltas.second) {
              auto err = co_await process_update(std::move(d));
              if (err != errc::success) {
                  vlog(
                    coproclog.warn,
                    "Result {} returned after processing update {}",
                    err,
                    deltas.first);
              }
          }
      });
}

ss::future<std::error_code>
reconciliation_backend::process_update(update_t delta) {
    using op_t = update_t::op_type;
    model::revision_id rev(delta.offset());
    switch (delta.type) {
    case op_t::add_non_replicable:
        if (!cluster::has_local_replicas(
              _self, delta.new_assignment.replicas)) {
            return ss::make_ready_future<std::error_code>(errc::success);
        }
        return create_non_replicable_partition(delta.ntp, rev);
    case op_t::del_non_replicable:
        return delete_non_replicable_partition(delta.ntp, rev).then([] {
            return std::error_code(errc::success);
        });
    default:
        /// All other case statements are no-ops because those events are
        /// expected to be handled in cluster::controller_backend. Convsersely
        /// the controller_backend will not handle the types of events that
        /// reconciliation_backend is responsible for
        return ss::make_ready_future<std::error_code>(errc::success);
    }
}

ss::future<> reconciliation_backend::delete_non_replicable_partition(
  model::ntp ntp, model::revision_id rev) {
    vlog(coproclog.info, "removing {} from shard table at {}", ntp, rev);
    co_await _shard_table.invoke_on_all(
      [ntp, rev](cluster::shard_table& st) { st.erase(ntp, rev); });
    auto copro_partition = _partition_manager.local().get(ntp);
    if (copro_partition && copro_partition->get_revision_id() < rev) {
        co_await _partition_manager.local().remove(ntp);
    }
}

ss::future<std::error_code>
reconciliation_backend::create_non_replicable_partition(
  model::ntp ntp, model::revision_id rev) {
    vlog(coproclog.info, "INVOCATION: {}", ntp);
    auto& topics_map = _topics.local().topics_map();
    auto tt_md = topics_map.find(model::topic_namespace_view(ntp));
    if (tt_md == topics_map.end()) {
        // partition was already removed, do nothing
        vlog(coproclog.info, "HHH2");
        co_return errc::success;
    }
    vassert(
      !tt_md->second.is_topic_replicable(),
      "Replicable topic reached non-replicable API");
    auto copro_partition = _partition_manager.local().get(ntp);
    if (likely(!copro_partition)) {
        // get_source_topic will assert if incorrect API is used
        auto src_partition = _pm.local().get(model::ntp(
          ntp.ns, tt_md->second.get_source_topic(), ntp.tp.partition));
        if (!src_partition) {
            vlog(coproclog.info, "HHH3");
            co_return errc::partition_not_exists;
        }
        auto ntp_cfg = tt_md->second.get_configuration().cfg.make_ntp_config(
          _data_directory, ntp.tp.partition, rev);
        co_await _partition_manager.local().manage(
          std::move(ntp_cfg), src_partition);
    } else if (copro_partition->get_revision_id() < rev) {
        vlog(coproclog.info, "HHH1");
        co_return errc::partition_already_exists;
    }
    co_await add_to_shard_table(std::move(ntp), ss::this_shard_id(), rev);
    co_return errc::success;
}

ss::future<> reconciliation_backend::add_to_shard_table(
  model::ntp ntp, ss::shard_id shard, model::revision_id revision) {
    vlog(
      coproclog.info, "adding {} to shard table at {}", revision, ntp, shard);
    return _shard_table.invoke_on_all(
      [ntp = std::move(ntp), shard, revision](cluster::shard_table& s) mutable {
          vassert(
            s.update_shard(ntp, shard, revision),
            "Newer revision for non-replicable ntp exists");
      });
}

} // namespace coproc
