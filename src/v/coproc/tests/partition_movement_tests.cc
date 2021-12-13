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
#include "coproc/tests/fixtures/coproc_cluster_fixture.h"
#include "coproc/tests/utils/batch_utils.h"
#include "coproc/tests/utils/coprocessor.h"

#include <boost/test/tools/old/interface.hpp>
#include <boost/test/unit_test_log.hpp>

using copro_typeid = coproc::registry::type_identifier;

/// Test moves across shards on same node works
// FIXTURE_TEST(test_move_source_topic, coproc_test_fixture) {
//     model::topic mvp("mvp");
//     setup({{mvp, 4}}).get();

//     auto id = coproc::script_id(443920);
//     enable_coprocessors(
//       {{.id = id(),
//         .data{
//           .tid = copro_typeid::identity_coprocessor,
//           .topics = {{mvp, coproc::topic_ingestion_policy::latest}}}}})
//       .get();

//     /// Push sample data onto all partitions
//     std::vector<ss::future<>> fs;
//     for (auto i = 0; i < 4; ++i) {
//         model::ntp input_ntp(
//           model::kafka_namespace, mvp, model::partition_id(i));
//         auto f = produce(input_ntp, make_random_batch(40));
//         fs.emplace_back(std::move(f));
//     }
//     ss::when_all_succeed(fs.begin(), fs.end()).get();

//     /// Wait until the materialized topic has come into existance
//     model::ntp origin_ntp(model::kafka_namespace, mvp,
//     model::partition_id(0)); model::ntp target_ntp = origin_ntp;
//     target_ntp.tp.topic = to_materialized_topic(
//       mvp, identity_coprocessor::identity_topic);
//     auto r = consume(target_ntp, 40).get();
//     BOOST_REQUIRE(num_records(r) == 40);
//     auto shard =
//     root_fixture()->app.shard_table.local().shard_for(target_ntp);
//     BOOST_REQUIRE(shard);

//     /// Choose target and calculate where to move it to
//     const ss::shard_id next_shard = (*shard + 1) % ss::smp::count;
//     info("Current target shard {} and next shard {}", *shard, next_shard);
//     model::broker_shard bs{
//       .node_id = model::node_id(config::node().node_id), .shard =
//       next_shard};

//     /// Move the input onto the new desired target
//     auto& topics_fe = root_fixture()->app.controller->get_topics_frontend();
//     auto ec = topics_fe.local()
//                 .move_partition_replicas(origin_ntp, {bs}, model::no_timeout)
//                 .get();
//     info("Error code: {}", ec);
//     BOOST_REQUIRE(!ec);

//     /// Wait until the shard table is updated with the new shard
//     tests::cooperative_spin_wait_with_timeout(
//       10s,
//       [this, target_ntp, next_shard] {
//           auto s = root_fixture()->app.shard_table.local().shard_for(
//             target_ntp);
//           return s && *s == next_shard;
//       })
//       .get();

//     /// Issue a read from the target shard, and expect the topic content
//     info("Draining from shard....{}", bs.shard);
//     r = consume(target_ntp, 40).get();
//     BOOST_CHECK_EQUAL(num_records(r), 40);

//     /// Finally, ensure there is no materialized partition on original shard
//     auto logf = root_fixture()->app.storage.invoke_on(
//       *shard, [origin_ntp](storage::api& api) {
//           return api.log_mgr().get(origin_ntp).has_value();
//       });
//     auto cpmf = root_fixture()->app.cp_partition_manager.invoke_on(
//       *shard, [origin_ntp](coproc::partition_manager& pm) {
//           return (bool)pm.get(origin_ntp);
//       });
//     BOOST_CHECK(!logf.get0());
//     BOOST_CHECK(!cpmf.get0());
// }

FIXTURE_TEST(test_move_topic_across_nodes, coproc_cluster_fixture) {
    const auto n = 3;
    create_node_application(model::node_id(0));
    create_node_application(model::node_id(1));
    create_node_application(model::node_id(2));
    wait_for_controller_leadership(model::node_id(0)).get();

    /// Find leader node
    auto leader_node_id = get_node_application(model::node_id(0))
                            ->controller->get_partition_leaders()
                            .local()
                            .get_leader(model::controller_ntp);
    BOOST_REQUIRE(leader_node_id.has_value());
    auto leader = get_node_fixture(*leader_node_id);

    model::topic_namespace tp(model::kafka_namespace, model::topic("source"));
    leader->add_topic(tp, 3).get();

    auto id = coproc::script_id(440);
    /// TODO: latest must allow reprocessing to occur
    enable_coprocessors(
      {{.id = id(),
        .data{
          .tid = copro_typeid::identity_coprocessor,
          .topics = {{tp.tp, coproc::topic_ingestion_policy::stored}}}}})
      .get();

    std::vector<ss::future<>> fs;
    for (auto i = 0; i < 4; ++i) {
        model::ntp input_ntp(
          model::kafka_namespace, tp.tp, model::partition_id(i));
        auto f = produce(input_ntp, make_random_batch(40));
        fs.emplace_back(std::move(f));
    }
    ss::when_all_succeed(fs.begin(), fs.end()).get();

    model::ntp origin_ntp(tp.ns, tp.tp, model::partition_id(0));
    model::ntp target_ntp(
      tp.ns,
      to_materialized_topic(tp.tp, identity_coprocessor::identity_topic),
      model::partition_id(0));
    auto r = consume(target_ntp, 40).get();
    BOOST_REQUIRE(num_records(r) == 40);

    /// Finally move the source topic after all processing by coproc has been
    /// completed. To do this, first find out what model::broker_shard the
    /// origin_ntp exists on, then attempt to move it to the node with the next
    /// node/shard id (mod n)
    auto topic_leader
      = leader->app.controller->get_partition_leaders().local().get_leader(
        origin_ntp);
    auto origin_shard = leader->app.shard_table.local().shard_for(origin_ntp);
    BOOST_REQUIRE(topic_leader.has_value());
    BOOST_REQUIRE(origin_shard.has_value());
    model::broker_shard old_bs{
      .node_id = *topic_leader, .shard = *origin_shard};
    model::broker_shard new_bs{
      .node_id = model::node_id((*topic_leader + 1) % n),
      .shard = ss::shard_id((*origin_shard + 1) % n)};

    info("Old broker_shard {} moving to new broker_shard {}", old_bs, new_bs);
    auto& topics_fe = leader->app.controller->get_topics_frontend();
    auto ec = topics_fe.local()
                .move_partition_replicas(
                  origin_ntp, {new_bs}, model::timeout_clock::now() + 5s)
                .get();
    info("Error code: {}", ec);
    BOOST_REQUIRE(!ec);

    /// To see if this worked correctly, issue a read from the target
    /// broker/shard after the move has completed
    r = consume(target_ntp, 40).get();
    BOOST_REQUIRE(num_records(r) == 40);

    /// Verify the new broker shard contains the ntps that were moved
    auto n1
      = leader->app.controller->get_partition_leaders().local().get_leader(
        origin_ntp);
    auto n2
      = leader->app.controller->get_partition_leaders().local().get_leader(
        target_ntp);
    BOOST_REQUIRE(n1.has_value());
    BOOST_REQUIRE(n2.has_value());
    BOOST_CHECK_EQUAL(*n1, *n2);

    auto s1 = leader->app.shard_table.local().shard_for(origin_ntp);
    auto s2 = leader->app.shard_table.local().shard_for(target_ntp);
    BOOST_REQUIRE(s1.has_value());
    BOOST_REQUIRE(s2.has_value());
    BOOST_CHECK_EQUAL(*s1, *s2);

    auto logf = get_node_application(new_bs.node_id)
                  ->storage.invoke_on(
                    new_bs.shard, [target_ntp](storage::api& api) {
                        return api.log_mgr().get(target_ntp).has_value();
                    });
    auto cpmf = get_node_application(new_bs.node_id)
                  ->cp_partition_manager.invoke_on(
                    new_bs.shard, [origin_ntp](coproc::partition_manager& pm) {
                        return (bool)pm.get(origin_ntp);
                    });
    BOOST_CHECK(logf.get());
    BOOST_CHECK(cpmf.get());
}
