/*
 * Copyright 2022 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once
#include "cluster/cluster_utils.h"
#include "cluster/members_table.h"
#include "cluster/partition_leaders_table.h"
#include "config/node_config.h"
#include "debug/logger.h"
#include "debug/self_test_gen_service.h"
#include "debug/types.h"
#include "rpc/connection_cache.h"
#include "rpc/types.h"

namespace debug {

enum class error_code {
    success = 0,
    failed = 1,
    not_leader = 2,
    no_leader_available = 3,
    partially_started = 4,
    peer_unreachable = 5,
    inconsistent_status = 6,
    not_started = 7,
    started = 8
};

/// API for interaction with the redpanda self test feature
class orchestrator {
public:
    static constexpr ss::shard_id shard = 0;

    orchestrator(
      model::node_id self,
      ss::sharded<cluster::members_table>& members,
      ss::sharded<cluster::partition_leaders_table>& leaders,
      ss::sharded<rpc::connection_cache>& connections);

    ss::future<> start() { return ss::now(); }
    ss::future<> stop() { return ss::now(); }

    ss::future<error_code> start_test(test_parameters parameters);
    ss::future<error_code> stop_test();
    ss::future<error_code> status();

private:
    template<typename Func>
    using invoke_peer_functor_t = typename std::
      invoke_result_t<Func, self_test_client_protocol>::get0_return_type;

    template<typename Func>
    using result_via_peer_t
      = std::pair<model::node_id, invoke_peer_functor_t<Func>>;

    struct test_info {
        test_id current_test_run{no_active_test};
        std::vector<model::node_id> participents;
    };

    ss::future<>
    initialize_peer_connection(model::node_id self, model::node_id id);

    template<typename Func>
    ss::future<std::vector<result_via_peer_t<Func>>> invoke_on_peers(
      model::node_id self, std::vector<model::node_id> peers, Func f) {
        std::vector<ss::future<result_via_peer_t<Func>>> r;
        for (const auto& node_id : peers) {
            vlog(dbg.info, "NODE ID: {}", node_id);
            if (!_connections.local().contains(node_id)) {
                co_await initialize_peer_connection(self, node_id);
            }
            r.push_back(_connections.local()
                          .with_node_client<self_test_client_protocol>(
                            self, ss::this_shard_id(), node_id, 1s, f)
                          .then([node_id](auto result) {
                              return std::make_pair(node_id, std::move(result));
                          }));
        }
        co_return co_await ss::when_all_succeed(r.begin(), r.end());
    }

    template<typename Func>
    ss::future<std::vector<result_via_peer_t<Func>>>
    invoke_on_all_peers(model::node_id self, Func f) {
        auto peers = _members.local().all_broker_ids();
        return invoke_on_peers(self, peers, std::move(f));
    }

private:
    test_info _info;

    /// All peers
    model::node_id _self;
    ss::sharded<cluster::members_table>& _members;
    ss::sharded<cluster::partition_leaders_table>& _leaders;
    ss::sharded<rpc::connection_cache>& _connections;
};

} // namespace debug
