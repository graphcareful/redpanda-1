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

#pragma once
#include "cluster/fwd.h"
#include "cluster/topic_table.h"
#include "coproc/fwd.h"
#include "coproc/script_context_router.h"
#include "coproc/types.h"
#include "storage/fwd.h"
#include "utils/mutex.h"

#include <seastar/core/gate.hh>
#include <seastar/core/sharded.hh>

#include <absl/container/node_hash_map.h>

#include <chrono>

namespace coproc {

class reconciliation_backend
  : public ss::peering_sharded_service<reconciliation_backend> {
public:
    explicit reconciliation_backend(
      ss::sharded<cluster::topic_table>&,
      ss::sharded<cluster::shard_table>&,
      ss::sharded<cluster::partition_manager>&,
      ss::sharded<partition_manager>&,
      ss::sharded<pacemaker>&,
      ss::sharded<wasm::script_database>&) noexcept;

    /// Starts the reconciliation loop
    ///
    /// Listens for events emitted from the topics table
    ss::future<> start();

    /// Stops the reconciliation loop
    ss::future<> stop();

private:
    using update_t = cluster::topic_table::delta;
    ss::future<std::error_code> process_update(update_t);
    ss::future<std::vector<update_t>>
      process_events_for_ntp(model::ntp, std::vector<update_t>);

    ss::future<std::error_code> process_shutdown(
      model::ntp, model::revision_id, std::vector<model::broker_shard>);
    ss::future<std::error_code> process_restart(model::ntp, model::revision_id);

    ss::future<> fetch_and_reconcile();

    ss::future<std::error_code>
    delete_non_replicable_partition(model::ntp ntp, model::revision_id rev);
    ss::future<std::error_code>
    create_non_replicable_partition(model::ntp ntp, model::revision_id rev);
    ss::future<> add_to_shard_table(
      model::ntp ntp, ss::shard_id shard, model::revision_id revision);

    template<typename Fn>
    ss::future<> within_context(Fn&&);

private:
    static constexpr auto retry_timeout_interval = std::chrono::milliseconds(
      100);

    /// Store x-core move state here, so logs may be re-opened instead of
    /// created, saves copro from having to reprocess entire logs from offset 0
    /// after move
    struct state_revision {
        absl::flat_hash_map<script_id, read_context> read_ctxs;
        model::revision_id r_id{};
    };
    absl::node_hash_map<model::ntp, state_revision> _saved_ctxs;

    cluster::notification_id_type _id_cb;
    model::node_id _self;
    ss::sstring _data_directory;
    absl::node_hash_map<model::ntp, std::vector<update_t>> _topic_deltas;

    mutex _mutex;
    ss::gate _gate;
    ss::abort_source _as;
    ss::timer<model::timeout_clock> _retry_timer;

    ss::sharded<cluster::topic_table>& _topics;
    ss::sharded<cluster::shard_table>& _shard_table;
    ss::sharded<cluster::partition_manager>& _cluster_pm;
    ss::sharded<partition_manager>& _coproc_pm;
    ss::sharded<pacemaker>& _pacemaker;
    ss::sharded<wasm::script_database>& _sdb;
};

} // namespace coproc
