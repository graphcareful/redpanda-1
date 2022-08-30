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

#include "describe_scram_credentials.h"

namespace kafka {
template<>
ss::future<response_ptr> describe_user_scram_credentials_topics_handler::handle(
  request_context ctx, [[maybe_unused]] ss::smp_service_group g) {
    describe_user_scram_credentials_response response;
    return ctx.respond(response);
}
} // namespace kafka
