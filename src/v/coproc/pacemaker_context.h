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

#include "coproc/supervisor.h"
#include "coproc/types.h"
#include "rpc/reconnect_transport.h"
#include "storage/api.h"
#include "storage/log.h"
#include "units.h"

#include <seastar/core/future.hh>
#include <seastar/core/semaphore.hh>

#include <absl/container/flat_hash_map.h>

namespace coproc {
/// Structure representing state about input topics that scripts will subscribe
/// to. ss::shared_pts to ntp_contexts will be used as there may be many
/// subscribers to an input ntp
struct ntp_context {
    explicit ntp_context(storage::log lg, const model::ntp& n, script_id id)
      : log(std::move(lg))
      , ntp(n) {
        offsets.emplace(id, model::offset(0));
    }

    /// Reference to the storage layer for reading from the input ntp
    storage::log log;
    /// ntp representing the 'log'
    model::ntp ntp;
    /// Interested scripts write their last read offset of the input ntp
    absl::flat_hash_map<script_id, model::offset> offsets;
};

using ntp_context_cache
  = absl::flat_hash_map<model::ntp, ss::lw_shared_ptr<ntp_context>>;

/// State to share across script_contexts per single shard
class pacemaker_context {
public:
    using rpc_result = result<rpc::client_context<process_batch_reply>>;

    explicit pacemaker_context(
      ss::sharded<storage::api>&, rpc::reconnect_transport&);

    /// Makes RPC to coproc engine, bounded by the '_send_sem' semaphore
    ss::future<rpc_result> send_request(process_batch_request);

    /// Append the record batch reader to the materialized log
    ss::future<bool> write_materialized(
      const model::materialized_ntp&, model::record_batch_reader);

    /// Aquire/release resources around source that reading from input logs
    ss::semaphore& read_sem() { return _read_sem; }

private:
    ss::future<result<supervisor_client_protocol>> get_client();

    ss::future<storage::log> get_log(const model::ntp&);

private:
    /// Max number of outbound requests allowed
    ss::semaphore _send_sem{4};

    /// Max amount of requests allowed to concurrently hold data in memory
    ss::semaphore _read_sem{20};

    /// Reference to the storage layer
    ss::sharded<storage::api>& _api;

    /// Details concerning connection to coproc engine, and reconnect logic
    uint8_t _connection_attempts{0};
    rpc::reconnect_transport& _transport;
};
} // namespace coproc
