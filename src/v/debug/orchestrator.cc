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

#include "debug/orchestrator.h"

#include "debug/logger.h"

namespace debug {

orchestrator::orchestrator(
  model::node_id self,
  ss::sharded<cluster::members_table>& members,
  ss::sharded<cluster::partition_leaders_table>& leaders,
  ss::sharded<rpc::connection_cache>& connections)
  : _self(self)
  , _members(members)
  , _leaders(leaders)
  , _connections(connections) {}

ss::future<orchestrator::id_or_failure>
orchestrator::start_test(test_parameters params) {
    const auto current_status = co_await status();
    if (current_status != error_code::not_started) {
        /// cannot start a test because there is either a connectivity issue or
        /// a test already running
        co_return current_status;
    }

    auto leader_opt = _leaders.local().get_leader(model::controller_ntp);

    if (!leader_opt.has_value()) {
        co_return error_code::no_leader_available;
    }

    if (leader_opt != _self) {
        co_return error_code::not_leader;
    }

    auto tid = uuid_t::create_random_uuid();
    auto peers = _members.local().all_broker_ids();
    auto results = co_await invoke_on_peers(
      _self, peers, [peers, params, tid](self_test_client_protocol c) {
          return c.start(
            self_test_start_request{
              .id = tid, .participants = peers, .parameters = params},
            rpc::client_opts(raft::clock_type::now() + 2s));
      });

    /// Perform a high level sanity check, allows client to know sooner if there
    /// is something wrong so it can quickly issue a subsequent stop command, in
    /// the case the test was partially deployed
    const auto succeeded = std::all_of(
      results.begin(), results.end(), [](const auto& r) {
          return r.second.value().data.ec == error_code::success;
      });
    if (!succeeded) {
        vlog(dbg.warn, "Failed to fully start self_test, id: {}", tid);
        co_return error_code::failed;
    }

    /// test id returned so client can correlate test runs with requests issued
    co_return tid;
}

ss::future<error_code> orchestrator::stop_test() {
    auto leader_opt = _leaders.local().get_leader(model::controller_ntp);
    if (!leader_opt.has_value()) {
        co_return error_code::no_leader_available;
    }
    if (leader_opt != _self) {
        co_return error_code::not_leader;
    }

    /// Use a longer timeout threshold to wait on asynchronous work performed on
    /// peers to be hard stopped
    auto results = co_await invoke_on_all_peers(
      _self, [](self_test_client_protocol c) {
          return c.stop(
            self_test_stop_request{},
            rpc::client_opts(raft::clock_type::now() + 10s));
      });

    /// Verify that the request was recieved and processed by peer
    const bool at_least_one_error = std::any_of(
      results.begin(), results.end(), [](const auto& reply) {
          return !reply.second.has_error();
      });

    co_return at_least_one_error ? error_code::peer_unreachable
                                 : error_code::success;
}

ss::future<error_code> orchestrator::status() {
    auto leader_opt = _leaders.local().get_leader(model::controller_ntp);
    if (!leader_opt.has_value()) {
        co_return error_code::no_leader_available;
    }
    if (leader_opt != _self) {
        co_return error_code::not_leader;
    }
    // Condense response from all peers to a known state about the system
    auto results = co_await invoke_on_all_peers(
      _self, [](self_test_client_protocol c) {
          return c.status(
            self_test_status_request{.query_status_only = true},
            rpc::client_opts(raft::clock_type::now() + 5s));
      });

    /// Either one or more peers are still processing the test, or none are
    /// There may also be connection errors encountered
    std::vector<test_id> ids;
    for (auto& [node_id, reply] : results) {
        if (reply.has_error()) {
            vlog(
              dbg.warn,
              "Could not reach broker with id {} when querying self-test "
              "status: {}",
              node_id,
              reply.error());
            co_return error_code::peer_unreachable;
        }
        ids.push_back(reply.value().data.id);
    }

    /// Move ids of brokers that aren't running tests to front of list
    auto running_itr = std::partition(
      ids.begin(), ids.end(), [](const auto& id) {
          return id == no_active_test;
      });

    /// Check that second partition of list has consistent ids
    auto consistent_itr = std::find_if(
      running_itr, ids.end(), [last = ids.back()](const auto& id) {
          return id != last;
      });

    if (consistent_itr != ids.end()) {
        /// not all peers believe they are running the same test, to
        /// mitigate, cancel all jobs then restart
        co_return error_code::inconsistent_status;
    }

    /// All peers agree the following to be the current test_id (if at least one
    /// job is running)
    const auto current_test_id = ids.back();
    if (current_test_id == no_active_test) {
        /// Test hasn't started, under this condition it is ok to call start()
        co_return error_code::not_started;
    }

    /// List of ids is mixed between not started ids, and some that all match,
    /// meaning that some (not all) brokers are still running jobs
    co_return error_code::running;
}

ss::future<> orchestrator::initialize_peer_connection(
  model::node_id self, model::node_id id) {
    auto broker = _members.local().get_broker(id);
    vassert(broker.has_value(), "Previously queried to exist");
    auto cert
      = co_await cluster::maybe_build_reloadable_certificate_credentials(
        config::node().rpc_server_tls());
    vlog(dbg.info, "Initializing connection to: {}", (*broker)->rpc_address());
    const auto shard = _connections.local().shard_for(
      self, ss::this_shard_id(), id);
    co_await _connections.invoke_on(
      shard,
      [id, cert, rpc_addr = (*broker)->rpc_address()](
        rpc::connection_cache& connections) {
          return connections.emplace(
            id,
            rpc::transport_configuration{
              .server_addr = rpc_addr,
              .credentials = cert,
              .disable_metrics = net::metrics_disabled(true)},
            rpc::make_exponential_backoff_policy<rpc::clock_type>(1s, 15s));
      });
}
} // namespace debug
