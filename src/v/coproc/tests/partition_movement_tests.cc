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

#include "coproc/partition_manager.h"
#include "coproc/tests/fixtures/coproc_bench_fixture.h"
#include "coproc/tests/utils/batch_utils.h"
#include "coproc/tests/utils/coprocessor.h"

#include <boost/test/tools/old/interface.hpp>
#include <boost/test/unit_test_log.hpp>

using copro_typeid = coproc::registry::type_identifier;

FIXTURE_TEST(test_move_source_topic, coproc_test_fixture) {
    model::topic mvp("mvp");
    setup({{mvp, 4}}).get();

    auto id = coproc::script_id(443920);
    enable_coprocessors(
      {{.id = id(),
        .data{
          .tid = copro_typeid::identity_coprocessor,
          .topics = {{mvp, coproc::topic_ingestion_policy::latest}}}}})
      .get();

    /// Push sample data onto all partitions
    std::vector<ss::future<>> fs;
    for (auto i = 0; i < 4; ++i) {
        model::ntp input_ntp(
          model::kafka_namespace, mvp, model::partition_id(i));
        auto f = produce(input_ntp, make_random_batch(40));
        fs.emplace_back(std::move(f));
    }
    ss::when_all_succeed(fs.begin(), fs.end()).get();

    /// Wait until the materialized topic has come into existance
    model::ntp origin_ntp(model::kafka_namespace, mvp, model::partition_id(0));
    model::ntp target_ntp = origin_ntp;
    target_ntp.tp.topic = to_materialized_topic(
      mvp, identity_coprocessor::identity_topic);
    auto r = consume(target_ntp, 40).get();
    BOOST_REQUIRE(num_records(r) == 40);
    auto shard = root_fixture()->app.shard_table.local().shard_for(target_ntp);
    BOOST_REQUIRE(shard);

    /// Choose target and calculate where to move it to
    const ss::shard_id next_shard = (*shard + 1) % ss::smp::count;
    info("Current target shard {} and next shard {}", *shard, next_shard);
    model::broker_shard bs{
      .node_id = model::node_id(config::node().node_id), .shard = next_shard};

    /// Move the input onto the new desired target
    auto& topics_fe = root_fixture()->app.controller->get_topics_frontend();
    auto ec = topics_fe.local()
                .move_partition_replicas(origin_ntp, {bs}, model::no_timeout)
                .get();
    info("Error code: {}", ec);
    BOOST_REQUIRE(!ec);

    /// Wait until the shard table is updated with the new shard
    tests::cooperative_spin_wait_with_timeout(
      10s,
      [this, target_ntp, next_shard] {
          auto s = root_fixture()->app.shard_table.local().shard_for(
            target_ntp);
          return s && *s == next_shard;
      })
      .get();

    /// Issue a read from the target shard, and expect the topic content
    info("Draining from shard....{}", bs.shard);
    r = consume(target_ntp, 40).get();
    BOOST_CHECK_EQUAL(num_records(r), 40);

    /// Finally, ensure there is no materialized partition on original shard
    auto logf = root_fixture()->app.storage.invoke_on(
      *shard, [origin_ntp](storage::api& api) {
          return api.log_mgr().get(origin_ntp).has_value();
      });
    auto cpmf = root_fixture()->app.cp_partition_manager.invoke_on(
      *shard, [origin_ntp](coproc::partition_manager& pm) {
          return (bool)pm.get(origin_ntp);
      });
    BOOST_CHECK(!logf.get0());
    BOOST_CHECK(!cpmf.get0());
}
