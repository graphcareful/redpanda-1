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

#include "kafka/protocol/request_reader.h"
#include "kafka/protocol/response_writer.h"
#include "kafka/protocol/schemata/describe_user_scram_credentials_request.h"
#include "kafka/protocol/schemata/describe_user_scram_credentials_response.h"
#include "seastarx.h"

#include <seastar/core/future.hh>

namespace kafka {

struct describe_user_scram_credentials_request final {
    using api_type = describe_user_scram_credentials_api;

    describe_user_scram_credentials_request_data data;

    void encode(response_writer& writer, api_version version) {
        data.encode(writer, version);
    }

    void decode(request_reader& reader, api_version version) {
        data.decode(reader, version);
    }

    friend std::ostream& operator<<(
      std::ostream& os, const describe_user_scram_credentials_request& r) {
        return os << r.data;
    }
};

struct describe_user_scram_credentials_response final {
    using api_type = describe_user_scram_credentials_api;

    describe_user_scram_credentials_response_data data;

    void encode(response_writer& writer, api_version version) {
        data.encode(writer, version);
    }

    void decode(iobuf buf, api_version version) {
        data.decode(std::move(buf), version);
    }

    friend std::ostream& operator<<(
      std::ostream& os, const describe_user_scram_credentials_response& r) {
        return os << r.data;
    }
};

} // namespace kafka
