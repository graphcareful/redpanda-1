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
#include "storage/fwd.h"

#include <seastar/core/gate.hh>
#include <seastar/core/semaphore.hh>
#include <seastar/core/sharded.hh>

namespace coproc {

class reconciliation_backend {
public:
    explicit reconciliation_backend(
      ss::sharded<cluster::topic_table>& topics,
      ss::sharded<cluster::shard_table>&,
      ss::sharded<storage::api>& storage,
      ss::sharded<pacemaker>&) noexcept;

    /// Starts the reconciliation loop
    ///
    /// Listens for events emitted from the topics table
    ss::future<> start();

    /// Stops the reconciliation loop
    ss::future<> stop();

private:
    using update_t = cluster::topic_table::non_rep_delta;
    ss::future<> process_update(update_t);
    ss::future<> process_updates(std::vector<update_t>);

    ss::future<> fetch_and_reconcile();

private:
    cluster::notification_id_type _id_cb;
    model::node_id _self;

    ss::gate _gate;
    ss::abort_source _as;
    ss::semaphore _sem{1};

    ss::sharded<cluster::topic_table>& _topics;
    ss::sharded<cluster::shard_table>& _shard_table;
    ss::sharded<storage::api>& _storage;
    ss::sharded<pacemaker>& _pacemaker;
};

} // namespace coproc
