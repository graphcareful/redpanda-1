/*
 * Copyright 2020 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "likely.h"
#include "raft/consensus.h"
#include "raft/raftgen_service.h"
#include "raft/types.h"
#include "seastarx.h"
#include "utils/copy_range.h"

#include <seastar/core/chunked_fifo.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/timed_out_error.hh>
#include <seastar/core/with_timeout.hh>

#include <absl/container/flat_hash_map.h>

#include <memory>
#include <vector>

namespace raft {
// clang-format off
template<typename ConsensusManager>
concept RaftGroupManager = requires(ConsensusManager m, group_id g) {
    { m.consensus_for(g) } -> std::same_as<ss::lw_shared_ptr<consensus>>;
};

template<typename ShardLookup>
concept ShardLookupManager = requires(ShardLookup m, group_id g) {
   { m.shard_for(g) } -> std::same_as<ss::shard_id>;
   { m.contains(g) } -> std::same_as<bool>;
};

template<typename ConsensusManager, typename ShardLookup>
  requires RaftGroupManager<ConsensusManager>
  && ShardLookupManager<ShardLookup>
// clang-format on
class service final : public raftgen_service {
public:
    using failure_probes = raftgen_service::failure_probes;
    service(
      ss::scheduling_group sc,
      ss::smp_service_group ssg,
      ss::sharded<ConsensusManager>& mngr,
      ShardLookup& tbl,
      clock_type::duration heartbeat_interval)
      : raftgen_service(sc, ssg)
      , _group_manager(mngr)
      , _shard_table(tbl)
      , _heartbeat_interval(heartbeat_interval) {
        finjector::shard_local_badger().register_probe(
          failure_probes::name(), &_probe);
    }
    ~service() override {
        finjector::shard_local_badger().deregister_probe(
          failure_probes::name());
    }

    [[gnu::always_inline]] ss::future<heartbeat_reply>
    heartbeat(heartbeat_request&& r, rpc::streaming_context&) final {
        using ret_t = std::vector<append_entries_reply>;
        auto const req_sz = r.heartbeats.size();
        auto grouped = group_hbeats_by_shard(std::move(r.heartbeats));

        std::vector<ss::future<std::vector<append_entries_reply>>> futures;
        futures.reserve(grouped.shard_requests.size());
        for (auto& [shard, req] : grouped.shard_requests) {
            // dispatch to each core in parallel
            futures.push_back(dispatch_hbeats_to_core(shard, std::move(req)));
        }
        // replies for groups that are not yet registered at this node
        std::vector<append_entries_reply> group_missing_replies;
        group_missing_replies.reserve(grouped.group_missing_requests.size());
        std::transform(
          std::begin(grouped.group_missing_requests),
          std::end(grouped.group_missing_requests),
          std::back_inserter(group_missing_replies),
          [](heartbeat_metadata& r) {
              return append_entries_reply{
                .group = r.meta.group,
                .result = append_entries_reply::status::group_unavailable};
          });

        return ss::when_all_succeed(futures.begin(), futures.end())
          .then([req_sz, missing = std::move(group_missing_replies)](
                  std::vector<ret_t> replies) mutable {
              ret_t ret;
              ret.reserve(req_sz);
              // flatten responses
              for (auto& part : replies) {
                  std::move(part.begin(), part.end(), std::back_inserter(ret));
              }
              std::move(
                missing.begin(), missing.end(), std::back_inserter(ret));
              return heartbeat_reply{std::move(ret)};
          });
    }

    [[gnu::always_inline]] ss::future<vote_reply>
    vote(vote_request&& r, rpc::streaming_context&) final {
        return _probe.vote().then([this, r = std::move(r)]() mutable {
            return dispatch_request(
              std::move(r),
              &service::make_failed_vote_reply,
              [](vote_request&& r, consensus_ptr c) {
                  return c->vote(std::move(r));
              });
        });
    }

    [[gnu::always_inline]] ss::future<append_entries_reply>
    append_entries(append_entries_request&& r, rpc::streaming_context&) final {
        return _probe.append_entries().then([this, r = std::move(r)]() mutable {
            auto gr = r.target_group();
            return dispatch_request(
              append_entries_request::make_foreign(std::move(r)),
              [gr]() { return make_missing_group_reply(gr); },
              [](append_entries_request&& r, consensus_ptr c) {
                  return c->append_entries(std::move(r));
              });
        });
    }
    [[gnu::always_inline]] ss::future<append_entries_reply>
    append_entries_full_serde(
      append_entries_request_serde_wrapper&& r, rpc::streaming_context&) final {
        return _probe.append_entries().then([this, r = std::move(r)]() mutable {
            auto request = std::move(r).release();
            const raft::group_id gr = request.target_group();
            return dispatch_request(
              append_entries_request::make_foreign(std::move(request)),
              [gr]() { return make_missing_group_reply(gr); },
              [](append_entries_request&& req, consensus_ptr c) {
                  return c->append_entries(std::move(req));
              });
        });
    }

    [[gnu::always_inline]] ss::future<install_snapshot_reply> install_snapshot(
      install_snapshot_request&& r, rpc::streaming_context&) final {
        return _probe.install_snapshot().then([this,
                                               r = std::move(r)]() mutable {
            return dispatch_request(
              std::move(install_snapshot_request_foreign_wrapper(std::move(r))),
              &service::make_failed_install_snapshot_reply,
              [](
                install_snapshot_request_foreign_wrapper&& r, consensus_ptr c) {
                  return c->install_snapshot(r.copy());
              });
        });
    }

    [[gnu::always_inline]] ss::future<timeout_now_reply>
    timeout_now(timeout_now_request&& r, rpc::streaming_context&) final {
        return _probe.timeout_now().then([this, r = std::move(r)]() mutable {
            return dispatch_request(
              std::move(r),
              &service::make_failed_timeout_now_reply,
              [](timeout_now_request&& r, consensus_ptr c) {
                  return c->timeout_now(std::move(r));
              });
        });
    }

    [[gnu::always_inline]] ss::future<transfer_leadership_reply>
    transfer_leadership(
      transfer_leadership_request&& r, rpc::streaming_context&) final {
        return _probe.transfer_leadership().then(
          [this, r = std::move(r)]() mutable {
              return dispatch_request(
                std::move(r),
                &service::make_failed_transfer_leadership_reply,
                [](transfer_leadership_request&& r, consensus_ptr c) {
                    return c->transfer_leadership(std::move(r));
                });
          });
    }

private:
    using consensus_ptr = seastar::lw_shared_ptr<consensus>;

    struct shard_groupped_hbeat_requests {
        absl::flat_hash_map<ss::shard_id, ss::chunked_fifo<heartbeat_metadata>>
          shard_requests;
        std::vector<heartbeat_metadata> group_missing_requests;
    };

    static ss::future<vote_reply> make_failed_vote_reply() {
        return ss::make_ready_future<vote_reply>(vote_reply{
          .term = model::term_id{}, .granted = false, .log_ok = false});
    }

    static ss::future<install_snapshot_reply>
    make_failed_install_snapshot_reply() {
        return ss::make_ready_future<install_snapshot_reply>(
          install_snapshot_reply{
            .term = model::term_id{}, .bytes_stored = 0, .success = false});
    }

    static ss::future<append_entries_reply>
    make_missing_group_reply(raft::group_id group) {
        return ss::make_ready_future<append_entries_reply>(append_entries_reply{
          .group = group,
          .result = append_entries_reply::status::group_unavailable});
    }

    static ss::future<timeout_now_reply> make_failed_timeout_now_reply() {
        return ss::make_ready_future<timeout_now_reply>(timeout_now_reply{});
    }

    static ss::future<transfer_leadership_reply>
    make_failed_transfer_leadership_reply() {
        return ss::make_ready_future<transfer_leadership_reply>(
          transfer_leadership_reply{});
    }

    template<typename Req, typename ErrorFactory, typename Func>
    auto dispatch_request(Req&& req, ErrorFactory&& ef, Func&& f) {
        auto group = req.target_group();
        if (unlikely(!_shard_table.contains(group))) {
            return ef();
        }
        auto shard = _shard_table.shard_for(group);
        return with_scheduling_group(
          get_scheduling_group(),
          [this,
           shard,
           r = std::forward<Req>(req),
           f = std::forward<Func>(f),
           ef = std::forward<ErrorFactory>(ef)]() mutable {
              return _group_manager.invoke_on(
                shard,
                get_smp_service_group(),
                [r = std::forward<Req>(r),
                 f = std::forward<Func>(f),
                 ef = std::forward<ErrorFactory>(ef)](
                  ConsensusManager& m) mutable {
                    auto c = m.consensus_for(r.target_group());
                    if (unlikely(!c)) {
                        return ef();
                    }
                    return f(std::forward<Req>(r), c);
                });
          });
    }

    ss::future<std::vector<append_entries_reply>> dispatch_hbeats_to_core(
      ss::shard_id shard, ss::chunked_fifo<heartbeat_metadata> heartbeats) {
        return with_scheduling_group(
          get_scheduling_group(),
          [this, shard, r = std::move(heartbeats)]() mutable {
              return _group_manager.invoke_on(
                shard,
                get_smp_service_group(),
                [this, r = std::move(r)](ConsensusManager& m) mutable {
                    return dispatch_hbeats_to_groups(m, std::move(r));
                });
          });
    }

    static append_entries_request
    make_append_entries_request(const heartbeat_metadata& hb) {
        return {
          hb.node_id,
          hb.target_node_id,
          hb.meta,
          model::make_memory_record_batch_reader(
            ss::circular_buffer<model::record_batch>{}),
          flush_after_append::no};
    }

    ss::future<std::vector<append_entries_reply>> dispatch_hbeats_to_groups(
      ConsensusManager& m, ss::chunked_fifo<heartbeat_metadata> reqs) {
        std::vector<ss::future<append_entries_reply>> futures;
        futures.reserve(reqs.size());
        // dispatch requests in parallel
        auto timeout = clock_type::now() + _heartbeat_interval;
        std::transform(
          reqs.begin(),
          reqs.end(),
          std::back_inserter(futures),
          [this, &m, timeout](heartbeat_metadata& hb) mutable {
              auto group = hb.meta.group;
              auto f = dispatch_append_entries(
                m, make_append_entries_request(hb));
              return ss::with_timeout(timeout, std::move(f))
                .handle_exception_type([group](const ss::timed_out_error&) {
                    return append_entries_reply{
                      .group = group,
                      .result = append_entries_reply::status::timeout};
                });
          });

        return ss::when_all_succeed(futures.begin(), futures.end());
    }

    shard_groupped_hbeat_requests
    group_hbeats_by_shard(std::vector<heartbeat_metadata> reqs) {
        shard_groupped_hbeat_requests ret;

        for (auto& r : reqs) {
            if (unlikely(!_shard_table.contains(r.meta.group))) {
                ret.group_missing_requests.push_back(r);
                continue;
            }

            auto shard = _shard_table.shard_for(r.meta.group);
            auto [it, _] = ret.shard_requests.try_emplace(shard);
            it->second.push_back(r);
        }

        return ret;
    }

    ss::future<append_entries_reply>
    dispatch_append_entries(ConsensusManager& m, append_entries_request&& r) {
        auto group = group_id(r.metadata().group);
        auto c = m.consensus_for(group);
        if (unlikely(!c)) {
            return make_missing_group_reply(group);
        }
        return c->append_entries(std::move(r));
    }

    failure_probes _probe;
    ss::sharded<ConsensusManager>& _group_manager;
    ShardLookup& _shard_table;
    clock_type::duration _heartbeat_interval;
};
} // namespace raft
