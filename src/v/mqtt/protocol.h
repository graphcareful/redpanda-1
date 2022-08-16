/*
 * Copyright 2020 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "mqtt/connection_context.h"
#include "net/server.h"
#include "seastarx.h"

#include <seastar/core/coroutine.hh>

namespace mqtt {

class protocol final : public net::server::protocol {
public:
    explicit protocol(ss::smp_service_group smp_group)
      : _smp_group(smp_group) {}

    ~protocol() noexcept override = default;
    protocol(const protocol&) = delete;
    protocol& operator=(const protocol&) = delete;
    protocol(protocol&&) noexcept = default;
    protocol& operator=(protocol&&) noexcept = delete;

    const char* name() const final { return "mqtt rpc protocol"; }

    // the lifetime of all references here are guaranteed to live
    // until the end of the server (container/parent)
    ss::future<> apply(net::server::resources rs) final {
        auto ctx = ss::make_lw_shared<connection_context>(*this, std::move(rs));
        co_return_co_await ss::do_until(
          [ctx] { return ctx->is_finished_parsing(); },
          [ctx] { return ctx->process_one_request(); })
          .handle_exception([](std::exception_ptr eptr) {
              vlog(mqlog.warn, "Error processing request: {}", eptr);
          });
    }

    ss::smp_service_group smp_group() const { return _smp_group; }

private:
    ss::smp_service_group _smp_group;
};

} // namespace mqtt
