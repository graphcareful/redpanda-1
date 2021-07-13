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

#include "coproc/ntp_context.h"
#include "coproc/types.h"

#include "utils/mutex.h"
#include <seastar/core/future.hh>
#include <absl/container/node_hash_map.h>

namespace coproc {
using output_write_inputs = std::vector<process_batch_reply::data>;

struct output_write_args {
    coproc::script_id id;
    storage::log_manager& log_manager;
    ntp_context_cache& inputs;
    absl::node_hash_map<model::ntp, mutex>& locks;
};

ss::future<>
  write_materialized(output_write_inputs, output_write_args);
} // namespace coproc
