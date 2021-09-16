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
#include "cluster/topic_table.h"
#include "cluster/types.h"
#include "coproc/pacemaker.h"
#include "coproc/script_database.h"
#include "storage/log_manager.h"

#include <seastar/core/gate.hh>
#include <seastar/core/sharded.hh>

namespace coproc {

class reconciliation_backend
  : public ss::peering_sharded_service<reconciliation_backend> {
public:
    explicit reconciliation_backend(
      model::node_id,
      storage::log_manager&,
      cluster::topic_table&,
      wasm::script_database&,
      ss::sharded<pacemaker>&) noexcept;

    ss::future<> start();
    ss::future<> stop();

private:
    using event_type = cluster::topic_table_delta::op_type;
    using deltas_t = std::vector<cluster::topic_table::delta>;
    using finishes_t
      = absl::node_hash_map<model::ntp, cluster::partition_assignment>;
    using moves_t = std::vector<model::ntp>;

    ss::future<> do_process_script_restarts(
      absl::node_hash_set<script_id>, model::ntp, ss::shard_id);
    ss::future<> do_process_restarts(finishes_t);
    ss::future<> process_restarts(finishes_t);
    ss::future<> process_shutdowns(moves_t);
    ss::future<> fetch_deltas();

private:
    static constexpr inline ss::shard_id copro_reconciliation_shard
      = ss::shard_id{0};

    model::node_id _self;

    storage::log_manager& _log_manager;
    cluster::topic_table& _topics;
    wasm::script_database& _sdb;

    ss::sharded<pacemaker>& _pacemaker;

    ss::semaphore _topics_sem{1};
    ss::gate _gate;
    ss::abort_source _as;
};

} // namespace coproc
