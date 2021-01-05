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

#include "coproc/pacemaker.h"

#include "cluster/namespace.h"
#include "coproc/types.h"
#include "model/validation.h"
#include "ssx/future-util.h"

#include <absl/container/flat_hash_set.h>

#include <algorithm>

namespace coproc {

pacemaker::pacemaker(ss::socket_address addr, ss::sharded<storage::api>& api)
  : _api(api)
  , _transport(
      {rpc::transport_configuration{
        .server_addr = addr, .credentials = nullptr}},
      rpc::make_exponential_backoff_policy<rpc::clock_type>(
        std::chrono::seconds(1), std::chrono::seconds(10))) {}

ss::future<> pacemaker::stop() {
    return _state_mtx
      .with([this] {
          std::vector<script_id> sids;
          sids.reserve(_scripts.size());
          for (const auto& [id, _] : _scripts) {
              sids.push_back(id);
          }
          return sids;
      })
      .then([this](std::vector<script_id> ids) {
          return ss::do_with(
            std::move(ids), [this](std::vector<script_id>& ids) {
                return ssx::async_all_of(
                  ids.begin(), ids.end(), [this](script_id id) {
                      return remove_source(id).then(
                        xform::equal_to(errc::success));
                  });
            });
      })
      .then([this](bool success) {
          if (!success) {
              vlog(
                coproclog.error,
                "Failed to gracefully shutdown all copro fibers");
          }
          vlog(coproclog.info, "Closing connection to coproc wasm engine");
          return _transport.stop();
      });
}

ss::future<std::vector<errc>> pacemaker::add_source(
  script_id id,
  std::vector<std::pair<model::topic, topic_ingestion_policy>> tnss) {
    std::vector<errc> acks;
    acks.reserve(tnss.size());
    return ss::do_with(
      ntp_context_cache(),
      std::move(acks),
      std::move(tnss),
      [this, id](
        ntp_context_cache& ctxs,
        std::vector<errc>& acks,
        std::vector<std::pair<model::topic, topic_ingestion_policy>>& tnss) {
          return _state_mtx
            .with([this, id, &ctxs, &acks, &tnss]() {
                vassert(
                  _scripts.find(id) == _scripts.end(),
                  "add_source() detects already existing script_id: {}",
                  id);
                return do_add_source(id, ctxs, acks, tnss)
                  .then([this, id, &ctxs]() {
                      if (ctxs.empty()) {
                          /// Failure occurred or no matching topics/ntps found
                          /// Reasons are returned in the 'acks' structure
                          return ss::now();
                      }
                      auto [itr, success] = _scripts.emplace(
                        std::piecewise_construct,
                        std::forward_as_tuple(id),
                        std::forward_as_tuple(id, _ctx, std::move(ctxs)));
                      vassert(success, "Double coproc insert detected");
                      return itr->second.start();
                  });
            })
            .then([&acks] { return std::move(acks); });
      });
}

errc check_topic_policy(const model::topic& topic, topic_ingestion_policy tip) {
    if (tip != topic_ingestion_policy::latest) {
        return errc::invalid_ingestion_policy;
    }
    if (model::is_materialized_topic(topic)) {
        return errc::materialized_topic;
    }
    if (model::validate_kafka_topic_name(topic).value() != 0) {
        return errc::invalid_topic;
    }
    return errc::success;
}

ss::future<> pacemaker::do_add_source(
  script_id id,
  ntp_context_cache& ctxs,
  std::vector<errc>& acks,
  const std::vector<std::pair<model::topic, topic_ingestion_policy>>& tnss) {
    return ss::do_for_each(
      tnss,
      [this, &ctxs, &acks, id](
        const std::pair<model::topic, topic_ingestion_policy>& item) {
          auto& [topic, tip] = item;
          const errc r = check_topic_policy(topic, tip);
          if (r != errc::success) {
              acks.push_back(r);
              return;
          }
          auto logs = _api.local().log_mgr().get(
            model::topic_namespace(cluster::kafka_namespace, topic));
          if (logs.empty()) {
              acks.push_back(errc::topic_does_not_exist);
              return;
          }
          ntp_context_cache data;
          for (auto& [ntp, log] : logs) {
              auto found = _ntps.find(ntp);
              if (found != _ntps.end()) {
                  auto ntp_ctx = found->second;
                  ntp_ctx->offsets.emplace(id, model::offset(0));
                  ctxs.emplace(ntp, ntp_ctx);
              } else {
                  auto ctx = ss::make_lw_shared<ntp_context>(log, ntp, id);
                  ctxs.emplace(ntp, ctx);
                  _ntps.emplace(ntp, ctx);
              }
          }
          acks.push_back(errc::success);
      });
}

ss::future<errc> pacemaker::remove_source(script_id id) {
    return _state_mtx.with([this, id] {
        auto found = _scripts.find(id);
        if (found == _scripts.end()) {
            return ss::make_ready_future<errc>(errc::script_id_does_not_exist);
        }
        script_context& ctx = found->second;
        return ctx.shutdown()
          .then([this] {
              /// shutdown explicity clears out strong references to ntp
              /// contexts. It is known to remove them from the pacemakers cache
              /// when the use_count() == 1, as there are then known to be no
              /// more subscribing scripts for the ntp
              absl::flat_hash_set<model::ntp> erase_set;
              for (auto& [ntp, ref] : _ntps) {
                  if (ref.use_count() == 1) {
                      erase_set.insert(ntp);
                  }
              }
              for (const model::ntp& ntp : erase_set) {
                  _ntps.erase(ntp);
              }
          })
          .then([this, found] {
              /// Finally, remove the script_context itself
              _scripts.erase(found);
              return errc::success;
          });
    });
}

ss::future<bool> pacemaker::local_script_id_exists(script_id id) {
    return _state_mtx.with(
      [this, id] { return _scripts.find(id) != _scripts.end(); });
}

ss::future<bool> pacemaker::ntp_is_registered(model::ntp ntp) {
    return _state_mtx.with([this, ntp = std::move(ntp)] {
        auto found = _ntps.find(ntp);
        return found != _ntps.end() && found->second;
    });
}

} // namespace coproc
