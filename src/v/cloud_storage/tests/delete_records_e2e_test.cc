/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "archival/ntp_archiver_service.h"
#include "cloud_storage/spillover_manifest.h"
#include "cloud_storage/tests/produce_utils.h"
#include "cloud_storage/tests/s3_imposter.h"
#include "config/configuration.h"
#include "kafka/server/tests/delete_records_utils.h"
#include "kafka/server/tests/produce_consume_utils.h"
#include "model/fundamental.h"
#include "redpanda/tests/fixture.h"
#include "storage/disk_log_impl.h"

#include <seastar/core/io_priority_class.hh>

#include <absl/container/flat_hash_set.h>
#include <boost/algorithm/string/predicate.hpp>
#include <boost/test/tools/old/interface.hpp>

#include <iterator>

using tests::kafka_consume_transport;
using tests::kafka_delete_records_transport;

static ss::logger e2e_test_log("delete_records_e2e_test");

namespace {

void check_consume_out_of_range(
  kafka_consume_transport& consumer,
  const model::topic& topic_name,
  const model::partition_id pid,
  model::offset kafka_offset) {
    BOOST_REQUIRE_EXCEPTION(
      consumer.consume_from_partition(topic_name, pid, kafka_offset).get(),
      std::runtime_error,
      [](std::runtime_error e) {
          return std::string(e.what()).find("out_of_range")
                 != std::string::npos;
      });
};

} // namespace

class delete_records_e2e_fixture
  : public s3_imposter_fixture
  , public redpanda_thread_fixture
  , public enable_cloud_storage_fixture {
public:
    static constexpr auto segs_per_spill = 10;
    delete_records_e2e_fixture()
      : redpanda_thread_fixture(
        redpanda_thread_fixture::init_cloud_storage_tag{},
        httpd_port_number()) {
        // No expectations: tests will PUT and GET organically.
        set_expectations_and_listen({});
        wait_for_controller_leadership().get();

        // Apply local retention frequently.
        config::shard_local_cfg().log_compaction_interval_ms.set_value(
          std::chrono::duration_cast<std::chrono::milliseconds>(1s));
        // We'll control uploads ourselves.
        config::shard_local_cfg()
          .cloud_storage_enable_segment_merging.set_value(false);
        config::shard_local_cfg()
          .cloud_storage_disable_upload_loop_for_tests.set_value(true);
        // Disable metrics to speed things up.
        config::shard_local_cfg().enable_metrics_reporter.set_value(false);
        // Encourage spilling over.
        config::shard_local_cfg()
          .cloud_storage_spillover_manifest_max_segments.set_value(
            std::make_optional<size_t>(segs_per_spill));

        topic_name = model::topic("tapioca");
        ntp = model::ntp(model::kafka_namespace, topic_name, 0);

        // Create a tiered storage topic with very little local retention.
        cluster::topic_properties props;
        props.shadow_indexing = model::shadow_indexing_mode::full;
        props.retention_local_target_bytes = tristate<size_t>(1);
        props.cleanup_policy_bitflags
          = model::cleanup_policy_bitflags::deletion;
        add_topic({model::kafka_namespace, topic_name}, 1, props).get();
        wait_for_leader(ntp).get();
        partition = app.partition_manager.local().get(ntp).get();
        log = dynamic_cast<storage::disk_log_impl*>(
          partition->log().get_impl());
        auto archiver_ref = partition->archiver();
        BOOST_REQUIRE(archiver_ref.has_value());
        archiver = &archiver_ref.value().get();
    }

    model::topic topic_name;
    model::ntp ntp;
    cluster::partition* partition;
    storage::disk_log_impl* log;
    archival::ntp_archiver* archiver;
};

// Test consuming after truncating the STM manifest.
FIXTURE_TEST(test_delete_from_stm_consume, delete_records_e2e_fixture) {
    // Create a segment with three distinct batches.
    tests::remote_segment_generator gen(make_kafka_client().get(), *partition);
    BOOST_REQUIRE_EQUAL(
      9, gen.num_segments(3).batches_per_segment(3).produce().get());
    BOOST_REQUIRE_EQUAL(3, archiver->manifest().size());

    // Delete in the middle of a segment.
    kafka_delete_records_transport deleter(make_kafka_client("deleter").get());
    deleter.start().get();
    auto lwm = deleter
                 .delete_records_from_partition(
                   topic_name, model::partition_id(0), model::offset(1), 5s)
                 .get();
    BOOST_CHECK_EQUAL(model::offset(1), lwm);
    tests::cooperative_spin_wait_with_timeout(3s, [this] {
        return log->segment_count() == 1;
    }).get();

    kafka_consume_transport consumer(make_kafka_client().get());
    consumer.start().get();
    auto consumed_records = consumer
                              .consume_from_partition(
                                topic_name,
                                model::partition_id(0),
                                model::offset(1))
                              .get();
    BOOST_CHECK_GE(consumed_records.size(), 2);
    BOOST_CHECK_EQUAL("key1", consumed_records[0].key);
    BOOST_CHECK_EQUAL("val1", consumed_records[0].val);
    BOOST_CHECK_EQUAL("key2", consumed_records[1].key);
    BOOST_CHECK_EQUAL("val2", consumed_records[1].val);
    check_consume_out_of_range(
      consumer, topic_name, model::partition_id(0), model::offset(0));
}

// Test consuming after truncating the archive manifests.
FIXTURE_TEST(test_delete_from_archive_consume, delete_records_e2e_fixture) {
    auto partition = app.partition_manager.local().get(ntp);
    auto& archiver = partition->archiver()->get();
    archiver.sync_for_tests().get();

    const auto records_per_seg = 5;
    const auto num_segs = 40;
    tests::remote_segment_generator gen(make_kafka_client().get(), *partition);
    auto total_records = gen.num_segments(num_segs)
                           .batches_per_segment(records_per_seg)
                           .produce()
                           .get();
    BOOST_REQUIRE_GE(total_records, 200);
    archiver.apply_spillover().get();
    auto& stm_manifest = archiver.manifest();
    BOOST_REQUIRE_EQUAL(
      stm_manifest.get_archive_start_offset(), model::offset(0));
    BOOST_REQUIRE_GT(
      stm_manifest.get_start_offset(), stm_manifest.get_archive_start_offset());
    BOOST_REQUIRE_EQUAL(
      stm_manifest.get_spillover_map().size(), num_segs / segs_per_spill - 1);
    BOOST_REQUIRE_EQUAL(stm_manifest.size(), segs_per_spill);
    BOOST_REQUIRE_EQUAL(
      archiver.upload_manifest("test").get(),
      cloud_storage::upload_result::success);
    ;
    archiver.flush_manifest_clean_offset().get();

    // Delete at every offset, ensuring we consume properly at each offset.
    kafka_delete_records_transport deleter(make_kafka_client().get());
    deleter.start().get();
    kafka_consume_transport consumer(make_kafka_client().get());
    consumer.start().get();
    for (size_t i = 1; i < total_records; i++) {
        auto lwm = deleter
                     .delete_records_from_partition(
                       topic_name, model::partition_id(0), model::offset(i), 5s)
                     .get();
        BOOST_REQUIRE_EQUAL(model::offset(i), lwm);
        check_consume_out_of_range(
          consumer, topic_name, model::partition_id(0), model::offset(i - 1));
        auto consumed_records = consumer
                                  .consume_from_partition(
                                    topic_name,
                                    model::partition_id(0),
                                    model::offset(i))
                                  .get();
        BOOST_REQUIRE(!consumed_records.empty());
        auto key = consumed_records[0].key;
        BOOST_REQUIRE_EQUAL(key, ssx::sformat("key{}", i));
    }
}

// Test that truncation is applied to cloud storage as expected when a
// DeleteRecords request lands in local storage.
FIXTURE_TEST(
  test_delete_from_local_storage_truncation, delete_records_e2e_fixture) {
    tests::remote_segment_generator gen(make_kafka_client().get(), *partition);
    size_t records_per_seg = 5;
    BOOST_REQUIRE_GE(
      250,
      gen.batches_per_segment(records_per_seg)
        .num_segments(40)
        .additional_local_segments(10)
        .produce()
        .get());
    archiver->apply_spillover().get();
    auto& stm_manifest = archiver->manifest();
    BOOST_REQUIRE_EQUAL(stm_manifest.get_spillover_map().size(), 3);
    BOOST_REQUIRE_EQUAL(stm_manifest.size(), segs_per_spill);

    // DeleteRecords to just before the end of the local log.
    kafka_delete_records_transport deleter(make_kafka_client().get());
    deleter.start().get();
    auto end_offset = kafka::offset(245);
    BOOST_REQUIRE_EQUAL(
      end_offset,
      deleter
        .delete_records_from_partition(
          topic_name,
          model::partition_id(0),
          kafka::offset_cast(end_offset),
          5s)
        .get());

    // The first housekeeping should remove all spillover segments.
    auto archive_start = stm_manifest.get_archive_start_offset();
    auto stm_start_before = stm_manifest.get_start_offset();
    BOOST_REQUIRE_EQUAL(archive_start, model::offset(0));
    archiver->housekeeping().get();
    BOOST_REQUIRE_EQUAL(
      stm_manifest.get_archive_start_offset(), model::offset{});
    BOOST_REQUIRE_EQUAL(10, archiver->manifest().size());
    BOOST_REQUIRE_EQUAL(stm_start_before, stm_manifest.get_start_offset());

    // The next should remove all STM segments.
    archiver->housekeeping().get();
    BOOST_REQUIRE(!archiver->manifest().get_start_offset().has_value());
}

// Test that truncation is applied to cloud storage as expected when a
// DeleteRecords request lands in the STM manifest.
FIXTURE_TEST(test_delete_from_stm_truncation, delete_records_e2e_fixture) {
    tests::remote_segment_generator gen(make_kafka_client().get(), *partition);
    size_t records_per_seg = 5;
    BOOST_REQUIRE_GE(
      250,
      gen.batches_per_segment(records_per_seg)
        .num_segments(40)
        .additional_local_segments(10)
        .produce()
        .get());
    archiver->apply_spillover().get();
    auto& stm_manifest = archiver->manifest();
    BOOST_REQUIRE_EQUAL(stm_manifest.get_spillover_map().size(), 3);
    BOOST_REQUIRE_EQUAL(stm_manifest.size(), segs_per_spill);
    kafka_delete_records_transport deleter(make_kafka_client().get());
    deleter.start().get();

    // Truncate within the STM at the bound, leaving one segment.
    auto stm_truncate_offset
      = archiver->manifest().get_last_kafka_offset().value();
    deleter
      .delete_records_from_partition(
        topic_name,
        model::partition_id(0),
        kafka::offset_cast(stm_truncate_offset),
        5s)
      .get();
    // The first housekeeping will cleanup the archive.
    archiver->housekeeping().get();
    BOOST_REQUIRE_EQUAL(10, archiver->manifest().size());
    BOOST_REQUIRE_EQUAL(
      stm_manifest.get_archive_start_offset(), model::offset{});

    // The next will clear the STM manifest.
    archiver->housekeeping().get();
    BOOST_REQUIRE_EQUAL(1, archiver->manifest().size());
    BOOST_REQUIRE_EQUAL(
      stm_manifest.get_archive_start_offset(), model::offset{});
}

// Test that truncation is applied to cloud storage as expected when a
// DeleteRecords request lands in the archive.
FIXTURE_TEST(test_delete_from_archive_truncation, delete_records_e2e_fixture) {
    tests::remote_segment_generator gen(make_kafka_client().get(), *partition);
    size_t records_per_seg = 5;
    BOOST_REQUIRE_GE(
      250,
      gen.batches_per_segment(records_per_seg)
        .num_segments(40)
        .additional_local_segments(10)
        .produce()
        .get());
    archiver->apply_spillover().get();
    auto& stm_manifest = archiver->manifest();
    BOOST_REQUIRE_EQUAL(stm_manifest.get_spillover_map().size(), 3);
    BOOST_REQUIRE_EQUAL(stm_manifest.size(), segs_per_spill);
    kafka_delete_records_transport deleter(make_kafka_client().get());
    deleter.start().get();

    // Truncate within the bounds of the first archive segment. Nothing should
    // be removed.
    auto& spillover = stm_manifest.get_spillover_map();
    auto first_seg_commit_offset
      = *spillover.get_committed_offset_column().at_index(0);
    auto first_seg_delta_end
      = *spillover.get_delta_offset_end_column().at_index(0);
    auto first_seg_last_kafka_offset = first_seg_commit_offset
                                       - first_seg_delta_end;
    deleter
      .delete_records_from_partition(
        topic_name,
        model::partition_id(0),
        model::offset(first_seg_last_kafka_offset),
        5s)
      .get();
    archiver->housekeeping().get();
    BOOST_REQUIRE_EQUAL(
      model::offset(0), stm_manifest.get_archive_start_offset());
    BOOST_REQUIRE_EQUAL(stm_manifest.get_spillover_map().size(), 3);
    BOOST_REQUIRE_EQUAL(stm_manifest.size(), segs_per_spill);

    // Now truncate just past the bounds of the first archive segment. It
    // should actually be removed.
    deleter
      .delete_records_from_partition(
        topic_name,
        model::partition_id(0),
        model::offset(first_seg_last_kafka_offset + 1),
        5s)
      .get();
    archiver->housekeeping().get();
    auto new_archive_start = stm_manifest.get_archive_start_offset();
    BOOST_REQUIRE_GT(new_archive_start, model::offset(0));
    BOOST_REQUIRE_EQUAL(stm_manifest.get_spillover_map().size(), 3);
    BOOST_REQUIRE_EQUAL(stm_manifest.size(), segs_per_spill);

    // Truncate right at the end of the archive. The archive should retain a
    // single segment.
    auto stm_kafka_start = stm_manifest.get_start_kafka_offset().value();
    deleter
      .delete_records_from_partition(
        topic_name,
        model::partition_id(0),
        model::offset(stm_kafka_start() - 1),
        5s)
      .get();
    archiver->housekeeping().get();
    BOOST_REQUIRE_GT(
      stm_manifest.get_archive_start_offset(), new_archive_start);
    BOOST_REQUIRE_EQUAL(stm_manifest.size(), segs_per_spill);
    // Truncate to the beginning of the STM manifest. This should remove the
    // archive entirely.
    deleter
      .delete_records_from_partition(
        topic_name,
        model::partition_id(0),
        model::offset(stm_kafka_start()),
        5s)
      .get();
    archiver->housekeeping().get();
    BOOST_REQUIRE_EQUAL(
      stm_manifest.get_archive_start_offset(), model::offset{});
    BOOST_REQUIRE_EQUAL(stm_manifest.size(), segs_per_spill);
}
