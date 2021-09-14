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

#include "coproc/tests/fixtures/coproc_bench_fixture.h"

#include <seastar/core/coroutine.hh>

coproc_bench_fixture::router_test_plan coproc_bench_fixture::build_simple_opts(
  log_layout_map llm,
  copro_map mapping,
  std::size_t input_rate,
  std::size_t output_rate) {
    router_test_plan rtp;
    /// inputs
    for (const auto& element : llm) {
        for (auto i = 0; i < element.second; ++i) {
            rtp.input[model::ntp(
              model::kafka_namespace, element.first, model::partition_id(i))]
              = router_test_plan::options{
                .number_of_batches = input_rate, .number_of_pushes = 1};
        }
    }
    /// outputs
    for (const auto& [topic, children] : mapping) {
        const auto found = llm.find(topic);
        vassert(found != llm.end(), "Error discovered in test plan");
        for (auto i = 0; i < found->second; ++i) {
            auto src = model::ntp(
              model::kafka_namespace, topic, model::partition_id(i));
            for (const auto& child : children) {
                auto ntp = model::ntp(
                  model::kafka_namespace, child, model::partition_id(i));
                rtp.output[ntp] = std::make_pair<>(
                  src,
                  router_test_plan::options{
                    .number_of_batches = output_rate, .number_of_pushes = 1});
            }
        }
    }
    return rtp;
}

ss::future<coproc_bench_fixture::drain_results>
coproc_bench_fixture::start_benchmark(router_test_plan plan) {
    co_await produce_all(std::move(plan.input));
    co_return co_await consume_all(std::move(plan.output));
}

ss::future<>
coproc_bench_fixture::produce_all(router_test_plan::input_test_plan plan) {
    return ss::do_with(std::move(plan), [this](auto& plan) {
        return ss::parallel_for_each(
          plan, [this](const auto& pair) -> ss::future<> {
              const auto& [src, options] = pair;
              for (auto i = 0; i < options.number_of_pushes; ++i) {
                  co_await produce(
                    src,
                    storage::test::make_random_memory_record_batch_reader(
                      model::offset{0}, options.number_of_batches, 1, false));
              }
          });
    });
}

ss::future<coproc_bench_fixture::drain_results>
coproc_bench_fixture::consume_all(router_test_plan::output_test_plan plan) {
    return ss::do_with(
      drain_results(),
      std::move(plan),
      [this](drain_results& results, auto& plan) {
          return ss::parallel_for_each(
                   plan,
                   [this, &results](const auto& pair) {
                       const auto& materialized = pair.first;
                       const auto& [src, options] = pair.second;
                       const std::size_t expected_batches
                         = options.number_of_pushes * options.number_of_batches;
                       return consume_materialized(
                                src, materialized, expected_batches)
                         .then([materialized, &results](auto drained_results) {
                             results.emplace(
                               materialized, drained_results.size());
                         });
                   })
            .then([&results] { return std::move(results); });
      });
}
