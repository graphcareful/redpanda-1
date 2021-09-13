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

#include "coproc/tests/fixtures/new_coproc_test_fixture.h"
#include "model/fundamental.h"

#include <absl/container/flat_hash_map.h>

#include <vector>

/// This harness brings up an entire redpanda fixture + the c++ implementation
/// of the wasm engine. Use this fixture for when a complete end-to-end
/// infrastructure is needed to perform some tests
class coproc_bench_fixture : public new_coproc_test_fixture {
public:
    struct router_test_plan {
        struct options {
            std::size_t number_of_batches{100};
            std::size_t number_of_pushes{1};
        };
        using input_test_plan = absl::flat_hash_map<model::ntp, options>;
        using output_test_plan
          = absl::flat_hash_map<model::ntp, std::pair<model::ntp, options>>;

        input_test_plan input;
        output_test_plan output;
    };
    using drain_results = absl::flat_hash_map<model::ntp, std::size_t>;

    using copro_map
      = absl::flat_hash_map<model::topic, std::vector<model::topic>>;

    ss::future<coproc_bench_fixture::drain_results>
      start_benchmark(router_test_plan);

    router_test_plan
      build_simple_opts(log_layout_map, copro_map, std::size_t, std::size_t);

private:
    ss::future<> produce_all(router_test_plan::input_test_plan);
    ss::future<drain_results> consume_all(router_test_plan::output_test_plan);
};
