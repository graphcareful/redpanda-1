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

#include "json/document.h"
#include "json/json.h"
#include "json/types.h"
#include "seastarx.h"
#include "utils/mutex.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/loop.hh>
#include <seastar/core/sleep.hh>

#include <chrono>

using namespace std::chrono_literals;

namespace self_test {

template<typename T>
class stub_test {
public:
    ss::future<T> start(ss::lowres_clock::time_point stoptime) {
        return ss::with_gate(_gate, [this, stoptime] {
            return ss::do_until(
                     [this, stoptime] {
                         return _as.abort_requested()
                                || (stoptime <= ss::lowres_clock::now());
                     },
                     [this] {
                         return ss::sleep_abortable(1s, _as)
                           .handle_exception_type(
                             [](const ss::sleep_aborted&) {});
                     })
              .then([stoptime]() {
                  return T(stoptime.time_since_epoch().count());
              });
        });
    }

    ss::future<> stop() {
        if (_as.abort_requested()) {
            if (_gate.is_closed()) {
                return ss::now();
            } else {
                return ss::make_exception_future<>(
                  std::runtime_error("Gate already closed"));
            }
        }
        _as.request_abort();
        return _gate.close();
    }

private:
    ss::gate _gate;
    ss::abort_source _as;
};

struct stub_result {
    long stoptime;
    explicit stub_result(long st)
      : stoptime(st) {}
};

class disk_test : public stub_test<stub_result> {};
class network_test : public stub_test<stub_result> {};

} // namespace self_test

namespace json {

inline void rjson_serialize(
  json::Writer<json::StringBuffer>& w, const self_test::stub_result& sr) {
    w.StartObject();
    w.Key("stoptime");
    json::rjson_serialize(w, static_cast<uint64_t>(sr.stoptime));
}

} // namespace json
