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

#include "coproc/tests/fixtures/coproc_test_fixture.h"

#include "config/configuration.h"
#include "coproc/logger.h"
#include "coproc/tests/utils/kafka_publish_consumer.h"
#include "kafka/protocol/schemata/produce_request.h"
#include "kafka/server/partition_proxy.h"
#include "model/record_batch_reader.h"
#include "model/timeout_clock.h"
#include "model/timestamp.h"
#include "test_utils/async.h"
#include "vassert.h"
#include "vlog.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/smp.hh>

#include <chrono>

namespace {

std::unique_ptr<kafka::client::client> make_client() {
    kafka::client::configuration cfg;
    cfg.brokers.set_value(std::vector<unresolved_address>{
      config::shard_local_cfg().kafka_api()[0].address});
    cfg.retries.set_value(size_t(1));
    return std::make_unique<kafka::client::client>(to_yaml(cfg));
}

} // namespace

coproc_test_fixture::coproc_test_fixture()
  : supervisor_test_fixture()
  , redpanda_thread_fixture() {
    ss::smp::invoke_on_all([]() {
        auto& config = config::shard_local_cfg();
        config.get("coproc_offset_flush_interval_ms").set_value(500ms);
    }).get0();
}

coproc_test_fixture::~coproc_test_fixture() {
    if (_client) {
        _client->stop().get();
    }
}

ss::future<>
coproc_test_fixture::enable_coprocessors(std::vector<deploy> copros) {
    std::vector<coproc::wasm::event> events;
    events.reserve(copros.size());
    std::transform(
      copros.begin(), copros.end(), std::back_inserter(events), [](deploy& e) {
          return coproc::wasm::event(e.id, std::move(e.data));
      });
    return ss::do_with(
      coproc::wasm::event_publisher(*_client),
      [events = std::move(events)](auto& eb) mutable {
          return eb.start().then([&eb, events = std::move(events)]() mutable {
              return eb
                .publish_events(coproc::wasm::make_event_record_batch_reader(
                  {std::move(events)}))
                .discard_result();
          });
      });
}

ss::future<>
coproc_test_fixture::disable_coprocessors(std::vector<uint64_t> ids) {
    std::vector<coproc::wasm::event> events;
    events.reserve(ids.size());
    std::transform(
      ids.begin(), ids.end(), std::back_inserter(events), [](uint64_t id) {
          return coproc::wasm::event(id);
      });
    return ss::do_with(
      coproc::wasm::event_publisher(*_client),
      [events = std::move(events)](auto& eb) mutable {
          return eb.start().then([&eb, events = std::move(events)]() mutable {
              return eb
                .publish_events(coproc::wasm::make_event_record_batch_reader(
                  {std::move(events)}))
                .discard_result();
          });
      });
}

ss::future<> coproc_test_fixture::setup(log_layout_map llm) {
    co_await wait_for_controller_leadership();
    for (auto& p : llm) {
        co_await add_topic(
          model::topic_namespace(model::kafka_namespace, p.first), p.second);
    }
    _client = make_client();
    co_await _client->connect();
}

ss::future<std::vector<kafka::produce_response::partition>>
coproc_test_fixture::produce(model::ntp ntp, model::record_batch_reader rbr) {
    vlog(coproc::coproclog.info, "About to produce to ntp: {}", ntp);
    auto results = co_await std::move(rbr).for_each_ref(
      kafka_publish_consumer(
        *_client, model::topic_partition{ntp.tp.topic, ntp.tp.partition}),
      model::no_timeout);
    for (const auto& r : results) {
        vassert(
          r.error_code != kafka::error_code::unknown_topic_or_partition,
          "Coproc test harness expects input logs to already be in existance, "
          "use setup() before starting your test");
    }
    co_return results;
}

ss::future<> coproc_test_fixture::wait_for_materialized(
  model::ntp ntp, std::chrono::milliseconds timeout) {
    const auto mgr_ntps = app.storage.local().log_mgr().get_all_ntps();
    if (mgr_ntps.find(ntp) != mgr_ntps.end()) {
        /// No need to wait, interested materialized partition exists
        return ss::now();
    }
    auto p = ss::make_lw_shared<ss::promise<>>();
    auto f = p->get_future();
    auto id = ss::make_lw_shared<model::notification_id_type>();
    *id = app.storage.local().log_mgr().register_manage_notification(
      ntp, [p](storage::log) mutable { p->set_value(); });
    auto to = ss::lowres_clock::now() + timeout;
    return ss::with_timeout(to, std::move(f))
      .handle_exception_type(
        [this, ntp, id, p](const ss::timed_out_error&) mutable {
            app.storage.local().log_mgr().unregister_manage_notification(*id);
            p->set_exception(ss::timed_out_error());
        })
      .then([this, id]() {
          app.storage.local().log_mgr().unregister_manage_notification(*id);
      });
}

ss::future<ss::stop_iteration> coproc_test_fixture::fetch_partition(
  model::record_batch_reader::data_t& events,
  model::offset& o,
  model::topic_partition tp,
  std::chrono::milliseconds timeout) {
    auto response = co_await _client->fetch_partition(tp, o, 64_KiB, timeout);
    if (response.data.error_code != kafka::error_code::none) {
        co_return ss::stop_iteration::yes;
    }
    vassert(response.data.topics.size() == 1, "Unexpected partition size");
    auto& p = response.data.topics[0];
    vassert(p.partitions.size() == 1, "Unexpected responses size");
    auto& pr = p.partitions[0];
    if (pr.error_code == kafka::error_code::none) {
        auto crs = kafka::batch_reader(std::move(*pr.records));
        while (!crs.empty()) {
            auto kba = crs.consume_batch();
            if (!kba.v2_format || !kba.valid_crc || !kba.batch) {
                continue;
            }
            events.push_back(std::move(*kba.batch));
            o = events.back().last_offset() + model::offset(1);
        }
    }
    co_return ss::stop_iteration::no;
};

ss::future<model::record_batch_reader::data_t>
coproc_test_fixture::do_consume_materialized(
  model::ntp ntp,
  std::size_t n_records,
  model::offset offset,
  std::chrono::milliseconds timeout) {
    vlog(coproc::coproclog.info, "Making request to fetch from ntp: {}", ntp);
    model::topic_partition tp{ntp.tp.topic, ntp.tp.partition};
    model::record_batch_reader::data_t events;
    ss::stop_iteration stop{};
    const auto start_time = model::timeout_clock::now();
    while (stop != ss::stop_iteration::yes && events.size() < n_records
           && ((model::timeout_clock::now() - start_time) < timeout)) {
        stop = co_await fetch_partition(events, offset, tp, timeout);
    }
    co_return events;
}

ss::future<model::record_batch_reader::data_t>
coproc_test_fixture::consume_materialized(
  model::ntp source,
  model::ntp materialized,
  std::size_t n_records,
  model::offset offset,
  std::chrono::milliseconds timeout) {
    auto shard_id = app.shard_table.local().shard_for(source);
    vassert(
      shard_id,
      "In coproc test harness source topics must be primed before test "
      "start");
    return app.storage
      .invoke_on(
        *shard_id,
        [this, materialized, timeout](storage::api&) {
            return wait_for_materialized(materialized, timeout);
        })
      .then([this, materialized, n_records, offset, timeout]() {
          /// TODO: Try to conditionally perform the call to update_metadata
          return _client->update_metadata().then(
            [this, materialized, n_records, offset, timeout] {
                return do_consume_materialized(
                  materialized, n_records, offset, timeout);
            });
      });
}

ss::future<> restartable_coproc_test_fixture::restart() {
    auto data_dir = _root_fixture->data_dir;
    _root_fixture->remove_on_shutdown = false;
    _root_fixture = nullptr;
    // _root_fixture = std::make_unique<coproc_test_fixture>(
    //   std::move(data_dir));
    co_await _root_fixture->wait_for_controller_leadership();
}
