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
#include "coproc/script_database.h"
#include "storage/api.h"

#include <seastar/core/coroutine.hh>

namespace {

std::optional<ss::shard_id>
new_shard(model::node_id self, std::vector<model::broker_shard> new_replicas) {
    auto rep = std::find_if(
      std::cbegin(new_replicas),
      std::cend(new_replicas),
      [self](const model::broker_shard& bs) { return bs.node_id == self; });
    return rep == new_replicas.end() ? std::nullopt
                                     : std::optional<ss::shard_id>(rep->shard);
}

std::vector<model::ntp> get_materialized_partitions(
  const cluster::topic_table& topics, const model::ntp& ntp) {
    std::vector<model::ntp> ntps;
    auto ps = topics.get_partition_assignment(ntp);
    if (ps) {
        auto found = topics.hierarchy_map().find(
          model::topic_namespace_view{ntp});
        if (found != topics.hierarchy_map().end()) {
            for (const auto& c : found->second) {
                ntps.emplace_back(c.ns, c.tp, ps->id);
            }
        }
    }
    return ntps;
}

using id_policy_map = absl::
  flat_hash_map<coproc::script_id, std::vector<coproc::topic_namespace_policy>>;
id_policy_map grab_metadata(
  coproc::wasm::script_database& sdb, model::topic_namespace_view tn_view) {
    id_policy_map results;
    auto found = sdb.find(tn_view);
    auto& ids = found->second;
    for (auto id : ids) {
        auto id_found = sdb.find(id);
        results.emplace(id, id_found->second.inputs);
    }
    return results;
}

} // namespace

namespace coproc {

reconciliation_backend::reconciliation_backend(
  ss::sharded<cluster::topic_table>& topics,
  ss::sharded<cluster::shard_table>& shard_table,
  ss::sharded<cluster::partition_manager>& cluster_pm,
  ss::sharded<partition_manager>& coproc_pm,
  ss::sharded<pacemaker>& pacemaker,
  ss::sharded<wasm::script_database>& sdb) noexcept
  : _self(model::node_id(config::node().node_id))
  , _data_directory(config::node().data_directory().as_sstring())
  , _topics(topics)
  , _shard_table(shard_table)
  , _cluster_pm(cluster_pm)
  , _coproc_pm(coproc_pm)
  , _pacemaker(pacemaker)
  , _sdb(sdb) {
    _retry_timer.set_callback([this] {
        (void)within_context([this]() { return fetch_and_reconcile(); });
    });
}

template<typename Fn>
ss::future<> reconciliation_backend::within_context(Fn&& fn) {
    try {
        return ss::with_gate(
          _gate, [this, fn = std::forward<Fn>(fn)]() mutable {
              if (!_as.abort_requested()) {
                  return _mutex.with(
                    [fn = std::forward<Fn>(fn)]() mutable { return fn(); });
              }
              return ss::now();
          });
    } catch (const ss::gate_closed_exception& ex) {
        vlog(coproclog.debug, "Timer fired during shutdown: {}", ex);
    }
    return ss::now();
}

ss::future<> reconciliation_backend::start() {
    _id_cb = _topics.local().register_delta_notification(
      [this](std::vector<update_t> deltas) {
          return within_context([this, deltas = std::move(deltas)]() mutable {
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
    _retry_timer.cancel();
    _as.request_abort();
    _topics.local().unregister_delta_notification(_id_cb);
    return _gate.close();
}

ss::future<std::vector<reconciliation_backend::update_t>>
reconciliation_backend::process_events_for_ntp(
  model::ntp ntp, std::vector<update_t> updates) {
    std::vector<update_t> retries;
    for (auto& d : updates) {
        vlog(coproclog.trace, "executing ntp: {} op: {}", d.ntp, d);
        auto err = co_await process_update(d);
        vlog(coproclog.info, "partition operation {} result {}", d, err);
        if (
          d.type == update_t::op_type::add_non_replicable
          && err == errc::partition_not_exists) {
            /// In this case the source topic exists but its
            /// associated partition doesn't, so try again
            retries.push_back(std::move(d));
        }
    }
    co_return retries;
}

ss::future<> reconciliation_backend::fetch_and_reconcile() {
    using deltas_cache = decltype(_topic_deltas);
    auto deltas = std::exchange(_topic_deltas, {});
    deltas_cache retry_cache;
    co_await ss::parallel_for_each(
      deltas.begin(),
      deltas.end(),
      [this, &retry_cache](deltas_cache::value_type& p) -> ss::future<> {
          auto retries = co_await process_events_for_ntp(p.first, p.second);
          if (!retries.empty()) {
              retry_cache[p.first] = std::move(retries);
          }
      });
    if (!retry_cache.empty()) {
        vlog(
          coproclog.warn,
          "There were recoverable errors when processing events, retrying");
        std::swap(_topic_deltas, retry_cache);
        if (!_retry_timer.armed()) {
            _retry_timer.arm(
              model::timeout_clock::now() + retry_timeout_interval);
        }
    }
}

ss::future<std::error_code>
reconciliation_backend::process_update(update_t delta) {
    using op_t = update_t::op_type;
    model::revision_id rev(delta.offset());
    const auto& replicas = delta.type == op_t::update
                             ? delta.previous_assignment->replicas
                             : delta.new_assignment.replicas;
    if (!cluster::has_local_replicas(_self, replicas)) {
        /// For the 'update' op type, perform this check against the new
        /// replica list
        return ss::make_ready_future<std::error_code>(errc::success);
    }

    switch (delta.type) {
    case op_t::add_non_replicable:
        return create_non_replicable_partition(delta.ntp, rev);
    case op_t::del_non_replicable:
        return delete_non_replicable_partition(delta.ntp, rev);
    case op_t::update:
        return process_shutdown(
          delta.ntp, rev, std::move(delta.new_assignment.replicas));
    case op_t::update_finished:
        return process_restart(delta.ntp, rev);
    case op_t::add:
    case op_t::del:
    case op_t::update_properties:
        /// All other case statements are no-ops because those events are
        /// expected to be handled in cluster::controller_backend. Convsersely
        /// the controller_backend will not handle the types of events that
        /// reconciliation_backend is responsible for
        return ss::make_ready_future<std::error_code>(errc::success);
    }
    __builtin_unreachable();
}

ss::future<std::error_code> reconciliation_backend::process_shutdown(
  model::ntp ntp,
  model::revision_id rev,
  std::vector<model::broker_shard> new_replicas) {
    vlog(coproclog.info, "Processing shutdown of: {}", ntp);

    auto ntps = get_materialized_partitions(_topics.local(), ntp);
    if (ntps.empty()) {
        /// Input doesn't exist on this broker/shard
        co_return make_error_code(errc::partition_not_exists);
    }

    auto rctxs = co_await _pacemaker.local().shutdown_partition(ntp);
    if (rctxs.empty()) {
        /// No coprocessors were processing on the shutdown input
        co_return make_error_code(errc::partition_not_exists);
    }

    /// Remove from shard table so ntp is effectively deregistered
    co_await _shard_table.invoke_on_all([ntps, rev](cluster::shard_table& st) {
        for (const auto& ntp : ntps) {
            st.erase(ntp, rev);
        }
    });

    /// Either remove from disk or move...
    std::optional<ss::shard_id> shard = new_shard(_self, new_replicas);
    if (!shard) {
        /// Move is occuring on a new node, just remove the log on this node
        for (const auto& ntp : ntps) {
            co_await _coproc_pm.local().remove(ntp);
            vlog(coproclog.info, "Materialized log {} removed", ntp);
        }
    } else {
        /// Move is occuring to a new shard on this node, call shutdown instead
        /// of remove to avoid erasing all log data
        for (const auto& ntp : ntps) {
            co_await _coproc_pm.local().shutdown(ntp);
            vlog(coproclog.info, "Materialized log {} shutdown", ntp);
        }
        /// Store information needed to re-create the log on new shard
        co_await this->container().invoke_on(
          *shard,
          [ntp, rev, rctxs = std::move(rctxs)](
            reconciliation_backend& be) mutable {
              auto [itr, success] = be._saved_ctxs.emplace(
                ntp, state_revision{});
              if (success || (!success && itr->second.r_id < rev)) {
                  vlog(
                    coproclog.info,
                    "Placing saved state on new shard: {}",
                    ntp);
                  itr->second = state_revision{
                    .read_ctxs = std::move(rctxs), .r_id = rev};
              }
          });
    }

    co_return make_error_code(errc::success);
}

ss::future<std::error_code> reconciliation_backend::process_restart(
  model::ntp ntp, model::revision_id rev) {
    vlog(coproclog.info, "Processing restart of: {}", ntp);

    auto ntps = get_materialized_partitions(_topics.local(), ntp);
    if (ntps.empty()) {
        /// Input doesn't exist on this broker/shard
        co_return make_error_code(errc::partition_not_exists);
    }

    state_revision sr;
    auto found = _saved_ctxs.extract(ntp);
    if (!found.empty()) {
        sr = std::move(found.mapped());
        vassert(
          sr.r_id < rev,
          "Older rev - ntp: {} old: {} new: {}",
          ntp,
          sr.r_id,
          rev);
    } else {
        sr.r_id = rev;
    }

    for (const auto& ntp : ntps) {
        vlog(coproclog.info, "Re-creating materialized log: {}", ntp);
        co_await create_non_replicable_partition(ntp, sr.r_id);
    }

    /// Obtain all matching script_ids for the shutdown partition, they must all
    /// be restarted
    auto meta = co_await _sdb.invoke_on(
      wasm::script_database_main_shard, [ntp](wasm::script_database& sdb) {
          return grab_metadata(sdb, model::topic_namespace_view{ntp});
      });

    using prps = pacemaker::restart_partition_state;
    absl::flat_hash_map<script_id, prps> r;
    for (auto& [id, policies] : meta) {
        auto rctx = sr.read_ctxs.find(id);
        if (rctx != sr.read_ctxs.end()) {
            /// x-core move detected for id, otherwise another node is hosting
            /// the partition
            r.emplace(id, prps{std::move(policies), rctx->second});
        }
    }

    /// Ensure that materialized log creation occurs before this line, as
    /// restart_partition will start processing and coprocessors would find
    /// themselves in an odd state (i.e. non_replicated topic exists in topic
    /// metadata but no associated storage::log exists)
    auto results = co_await _pacemaker.local().restart_partition(
      ntp, std::move(r));
    for (const auto& [id, errc] : results) {
        vlog(
          coproclog.info,
          "Upon restart of '{}' reported status: {}",
          ntp,
          errc);
    }
    co_return make_error_code(errc::success);
}

ss::future<std::error_code>
reconciliation_backend::delete_non_replicable_partition(
  model::ntp ntp, model::revision_id rev) {
    vlog(coproclog.trace, "removing {} from shard table at {}", ntp, rev);
    co_await _shard_table.invoke_on_all(
      [ntp, rev](cluster::shard_table& st) { st.erase(ntp, rev); });
    auto copro_partition = _coproc_pm.local().get(ntp);
    if (copro_partition && copro_partition->get_revision_id() < rev) {
        co_await _pacemaker.local().with_hold(
          copro_partition->source(), ntp, [this, ntp]() {
              return _coproc_pm.local().remove(ntp);
          });
    }
    co_return make_error_code(errc::success);
}

ss::future<std::error_code>
reconciliation_backend::create_non_replicable_partition(
  model::ntp ntp, model::revision_id rev) {
    auto& topics_map = _topics.local().topics_map();
    auto tt_md = topics_map.find(model::topic_namespace_view(ntp));
    if (tt_md == topics_map.end()) {
        // partition was already removed, do nothing
        co_return errc::success;
    }
    vassert(
      !tt_md->second.is_topic_replicable(),
      "Expected to find non_replicable_topic instead: {}",
      ntp);
    auto copro_partition = _coproc_pm.local().get(ntp);
    if (likely(!copro_partition)) {
        // get_source_topic will assert if incorrect API is used
        auto src_partition = _cluster_pm.local().get(model::ntp(
          ntp.ns, tt_md->second.get_source_topic(), ntp.tp.partition));
        if (!src_partition) {
            co_return errc::partition_not_exists;
        }
        auto ntp_cfg = tt_md->second.get_configuration().cfg.make_ntp_config(
          _data_directory, ntp.tp.partition, rev);
        co_await _coproc_pm.local().manage(std::move(ntp_cfg), src_partition);
    } else if (copro_partition->get_revision_id() < rev) {
        co_return errc::partition_already_exists;
    }
    co_await add_to_shard_table(std::move(ntp), ss::this_shard_id(), rev);
    co_return errc::success;
}

ss::future<> reconciliation_backend::add_to_shard_table(
  model::ntp ntp, ss::shard_id shard, model::revision_id revision) {
    vlog(
      coproclog.trace,
      "adding {} / {} to shard table at {}",
      revision,
      ntp,
      shard);
    return _shard_table.invoke_on_all(
      [ntp = std::move(ntp), shard, revision](cluster::shard_table& s) mutable {
          vassert(
            s.update_shard(ntp, shard, revision),
            "Newer revision for non-replicable ntp {} exists: {}",
            ntp,
            revision);
      });
}

} // namespace coproc
