/*
 * Copyright 2020 Vectorized, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "coproc/pacemaker_context.h"
#include "coproc/script_context.h"
#include "coproc/types.h"
#include "rpc/reconnect_transport.h"
#include "storage/api.h"
#include "utils/mutex.h"

#include <seastar/core/sharded.hh>
#include <seastar/net/inet_address.hh>
#include <seastar/net/socket_defs.hh>

#include <absl/container/flat_hash_map.h>

#include <unordered_map>

namespace coproc {
/**
 * Sharded service that manages the registration of scripts to registered
 * topics. Each script is its own 'fiber', containing its own event loop which
 * reads data from its interested topics, sends to the wasm engine, and
 * processes the response (almost always a write to a materialized log)
 */
class pacemaker {
public:
    /**
     * class constructor
     * @param address of coprocessor engine
     * @param reference to the storage layer
     */
    pacemaker(ss::socket_address, ss::sharded<storage::api>&);

    /**
     * Gracefully stops and deregisters all coproc scripts
     */
    ss::future<> stop();

    /**
     * Registers a new coproc script into the system
     *
     * @param script_id You must verify that this is unique by querying all
     shards. A shard only contains an entry for a script id if there is at least
     one partition of a topic for that shard.
     * @param topics vector of pairs of topics and their registration policies
     * @returns a vector of error codes per input topic corresponding to the
     * order of the topic vector passed to this method
     */
    ss::future<std::vector<errc>> add_source(
      script_id, std::vector<std::pair<model::topic, topic_ingestion_policy>>);

    /**
     * Removes a script_id from the pacemaker. Shuts down relevent fibers and
     * deregisters ntps that no longer have any registered interested scripts
     *
     * @param script_id The identifer of the script desired to be shutdown
     */
    ss::future<errc> remove_source(script_id);

    /**
     * @returns true if a matching script id exists on 'this' shard
     */
    ss::future<bool> local_script_id_exists(script_id);

    /**
     * @returns true if a matching ntp exists on 'this' shard
     */
    ss::future<bool> ntp_is_registered(model::ntp);

private:
    ss::future<> do_add_source(
      script_id,
      ntp_context_cache&,
      std::vector<errc>& acks,
      const std::vector<std::pair<model::topic, topic_ingestion_policy>>&);

private:
    /// Reference to the storage layer used to lookup registered ntps
    ss::sharded<storage::api>& _api;

    /// Connection to the coprocessor engine
    rpc::reconnect_transport _transport;

    /// Lock to be used when mutating the '_scripts' and '_ntps' data
    /// structures
    mutex _state_mtx;
    std::unordered_map<script_id, script_context> _scripts;
    ntp_context_cache _ntps;

    /// State to be shared across execution units i.e. script_contexts
    pacemaker_context _ctx{_api, _transport};
};

} // namespace coproc
