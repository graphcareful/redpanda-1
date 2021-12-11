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

#include "cluster/tests/cluster_test_fixture.h"
#include "coproc/tests/fixtures/coproc_test_fixture.h"
#include "coproc/tests/fixtures/supervisor_test_fixture.h"
#include "coproc/tests/utils/event_publisher_utils.h"

#include <absl/container/flat_hash_map.h>

/// Test that moves across nodes works
class coproc_cluster_fixture
  : public cluster_test_fixture
  , public coproc_api_fixture {
public:
    using wasm_ptr = std::unique_ptr<supervisor_test_fixture>;

    coproc_cluster_fixture() noexcept
      : cluster_test_fixture()
      , coproc_api_fixture() {
        set_configuration("enable_coproc", true);
    }

    ss::future<> enable_coprocessors(std::vector<deploy> copros) {
        std::vector<coproc::script_id> ids;
        std::vector<ss::future<>> wait;
        std::transform(
          copros.cbegin(),
          copros.cend(),
          std::back_inserter(ids),
          [](const deploy& d) { return coproc::script_id(d.id); });
        co_await coproc_api_fixture::enable_coprocessors(std::move(copros));
        auto node_ids = get_node_ids();
        co_await ss::parallel_for_each(
          node_ids, [this, ids](const model::node_id& node_id) {
              application* app = get_node_application(node_id);
              return ss::parallel_for_each(
                ids, [this, app](coproc::script_id id) {
                    return coproc::wasm::wait_for_copro(
                      app->coprocessing->get_pacemaker(), id);
                });
          });
    }

    application* create_node_application(
      model::node_id node_id,
      int kafka_port = 9092,
      int rpc_port = 11000,
      int proxy_port = 8082,
      int schema_reg_port = 8081,
      int coproc_supervisor_port = 43189) {
        application* app = cluster_test_fixture::create_node_application(
          node_id,
          kafka_port,
          rpc_port,
          proxy_port,
          schema_reg_port,
          coproc_supervisor_port);
        _instances.emplace(
          node_id,
          std::make_unique<supervisor_test_fixture>(
            coproc_supervisor_port + node_id()));
        return app;
    }

    std::vector<model::node_id> get_node_ids() {
        std::vector<model::node_id> ids;
        std::transform(
          _instances.cbegin(),
          _instances.cend(),
          std::back_inserter(ids),
          [](const auto& p) { return p.first; });
        return ids;
    }

private:
    absl::flat_hash_map<model::node_id, wasm_ptr> _instances;
};
