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

namespace mqtt {
class connection_context final
  : public ss::enable_lw_shared_from_this<connection_context> {
public:
    connection_context(protocol& p, net::server::resources&& r) {}
};

} // namespace mqtt
