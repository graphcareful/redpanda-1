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
#include "utils/uuid.h"

#include <seastar/core/sstring.hh>

#include <absl/container/flat_hash_map.h>
#include <serde/serde.h>

namespace debug {

using test_results = named_type<
  absl::flat_hash_map<ss::sstring, ss::sstring>,
  struct self_test_results_tag>;

enum class error_code {
    success = 0,
    failed = 1,
    not_leader = 2,
    no_leader_available = 3,
    partially_started = 4,
    peer_unreachable = 5,
    inconsistent_error_code = 6,
    not_running = 7,
    in_progress = 8
};

struct test_parameters : serde::envelope<test_parameters, serde::version<0>> {
    std::chrono::seconds disk_test_timeout_sec;
    std::chrono::seconds network_test_timeout_sec;
};

struct self_test_start_request
  : serde::envelope<self_test_start_request, serde::version<0>> {
    using rpc_adl_exempt = std::true_type;
    uuid_t id;
    std::vector<model::node_id> participants;
    test_parameters parameters;

    auto serde_fields() { return std::tie(id, participants, parameters); }
};

struct self_test_start_reply
  : serde::envelope<self_test_start_reply, serde::version<0>> {
    using rpc_adl_exempt = std::true_type;
    uuid_t id;
    error_code ec;

    auto serde_fields() { return std::tie(id, ec); }
};

struct self_test_stop_request
  : serde::envelope<self_test_stop_request, serde::version<0>> {
    using rpc_adl_exempt = std::true_type;
    int8_t empty;

    auto serde_fields() { return std::tie(empty); }
};

struct self_test_stop_reply
  : serde::envelope<self_test_stop_reply, serde::version<0>> {
    using rpc_adl_exempt = std::true_type;
    uuid_t id;
    error_code s;

    auto serde_fields() { return std::tie(id, s); }
};

struct self_test_error_code_request
  : serde::envelope<self_test_error_code_request, serde::version<0>> {
    using rpc_adl_exempt = std::true_type;
    bool query_error_code_only{true};

    auto serde_fields() { return std::tie(query_error_code_only); }
};

struct self_test_error_code_reply
  : serde::envelope<self_test_error_code_reply, serde::version<0>> {
    using rpc_adl_exempt = std::true_type;
    uuid_t id;
    error_code s;
    test_results disk_test_result;
    test_results network_test_result;

    auto serde_fields() {
        return std::tie(id, disk_test_result, network_test_result);
    }
};

} // namespace debug
