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
#include "model/metadata.h"
#include "seastarx.h"
#include "utils/named_type.h"

#include <seastar/core/sstring.hh>

#include <absl/container/flat_hash_map.h>
#include <serde/serde.h>

namespace debug {

using test_id = named_type<int64_t, struct self_test_id_tag>;
using test_name = named_type<ss::sstring, struct self_test_name_tag>;
using test_results = named_type<
  absl::flat_hash_map<ss::sstring, ss::sstring>,
  struct self_test_results_tag>;

static constexpr auto no_active_test = test_id(-1);

enum class status {
    not_started = 0,
    started = 1,
    in_progress = 2,
    finished = 3
};

struct test_parameters : serde::envelope<test_parameters, serde::version<0>> {
    std::chrono::seconds disk_test_timeout_sec;
    std::chrono::seconds network_test_timeout_sec;
};

struct self_test_start_request
  : serde::envelope<self_test_start_request, serde::version<0>> {
    using rpc_adl_exempt = std::true_type;
    test_id id;
    std::vector<model::node_id> participants;
    test_parameters parameters;

    auto serde_fields() { return std::tie(id, participants, parameters); }
};

struct self_test_start_reply
  : serde::envelope<self_test_start_reply, serde::version<0>> {
    using rpc_adl_exempt = std::true_type;
    test_id id;
    status current_job;

    auto serde_fields() { return std::tie(id, current_job); }
};

struct self_test_stop_request
  : serde::envelope<self_test_stop_request, serde::version<0>> {
    using rpc_adl_exempt = std::true_type;
    test_id id;

    auto serde_fields() { return std::tie(id); }
};

struct self_test_stop_reply
  : serde::envelope<self_test_stop_reply, serde::version<0>> {
    using rpc_adl_exempt = std::true_type;
    int8_t empty;

    auto serde_fields() { return std::tie(empty); }
};

struct self_test_status_request
  : serde::envelope<self_test_status_request, serde::version<0>> {
    using rpc_adl_exempt = std::true_type;
    test_id id;
    bool query_status_only{true};

    auto serde_fields() { return std::tie(id, query_status_only); }
};

struct self_test_status_reply
  : serde::envelope<self_test_status_reply, serde::version<0>> {
    using rpc_adl_exempt = std::true_type;
    test_id id;
    test_results disk_test_result;
    test_results network_test_result;

    auto serde_fields() {
        return std::tie(id, disk_test_result, network_test_result);
    }
};

} // namespace debug
