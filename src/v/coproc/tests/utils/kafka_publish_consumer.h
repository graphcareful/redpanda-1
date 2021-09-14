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
#include "kafka/client/client.h"
#include "kafka/protocol/schemata/produce_response.h"
#include "model/fundamental.h"

#include <seastar/core/coroutine.hh>

#include <vector>

class kafka_publish_consumer {
public:
    explicit kafka_publish_consumer(
      kafka::client::client& client, model::topic_partition tp)
      : _client(client)
      , _publish_tp(std::move(tp)) {}

    ss::future<ss::stop_iteration> operator()(model::record_batch& rb) {
        _results.push_back(
          co_await _client.produce_record_batch(_publish_tp, std::move(rb)));
        co_return ss::stop_iteration::no;
    }

    std::vector<kafka::produce_response::partition> end_of_stream() {
        return std::move(_results);
    }

private:
    std::vector<kafka::produce_response::partition> _results;
    kafka::client::client& _client;
    model::topic_partition _publish_tp;
};
