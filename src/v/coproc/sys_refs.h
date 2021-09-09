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

#include "cluster/partition_manager.h"
#include "storage/api.h"

#include <seastar/core/sharded.hh>

namespace coproc {

/// Struct of references of external layers of redpanda that coproc will
/// leverage
struct sys_refs {
    ss::sharded<storage::api>& storage;
    ss::sharded<cluster::partition_manager>& partition_manager;
};

} // namespace coproc
