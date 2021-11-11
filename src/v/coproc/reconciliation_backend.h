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
      ss::sharded<cluster::topic_table>&,
      ss::sharded<cluster::shard_table>&,
      ss::sharded<cluster::partition_manager>&,
      ss::sharded<partition_manager>&) noexcept;

    /// Starts the reconciliation loop
    ///
    /// Listens for events emitted from the topics table
    ss::future<> start();

    /// Stops the reconciliation loop
    ss::future<> stop();

private:
    using update_t = cluster::topic_table::delta;
    ss::future<std::error_code> process_update(update_t);

    ss::future<> fetch_and_reconcile();

    ss::future<>
    delete_non_replicable_partition(model::ntp ntp, model::revision_id rev);
    ss::future<std::error_code>
    create_non_replicable_partition(model::ntp ntp, model::revision_id rev);
    ss::future<> add_to_shard_table(
      model::ntp ntp, ss::shard_id shard, model::revision_id revision);

private:
    cluster::notification_id_type _id_cb;
    model::node_id _self;
    ss::sstring _data_directory;
    absl::flat_hash_map<model::ntp, std::vector<update_t>> _topic_deltas;

    ss::gate _gate;
    ss::abort_source _as;
    ss::semaphore _sem{1};

    ss::sharded<cluster::topic_table>& _topics;
    ss::sharded<cluster::shard_table>& _shard_table;
    ss::sharded<cluster::partition_manager>& _pm;
    ss::sharded<partition_manager>& _partition_manager;
};

} // namespace coproc
