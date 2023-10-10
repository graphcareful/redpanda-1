/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once
#include "net/unresolved_address.h"
#include "security/audit/schemas/application_activity.h"
#include "security/audit/schemas/types.h"
#include "utils/string_switch.h"

#include <seastar/http/request.hh>

namespace security::audit {

struct authorization_denied {
    ss::sstring msg;
};

struct authorization_allowed {
    ss::sstring username;
    ss::sstring mechanism;
};

/// Create an security::audit::api_activity event from an ss::http::request
///
/// Fills the appropriate structures for an unauthorized connect attempt event
api_activity make_api_activity_event(
  const ss::http::request& req, const authorization_denied& denied);

/// Create an security::audit::api_activity event from an ss::http::request
///
/// Fills the appropriate structures for an authorized connection event
api_activity make_api_activity_event(
  const ss::http::request& req, const authorization_allowed& allowed);

} // namespace security::audit
