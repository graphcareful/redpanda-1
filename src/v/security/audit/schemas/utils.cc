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

#include "security/audit/schemas/utils.h"

namespace {

namespace sa = security::audit;

sa::api_activity::activity_id
http_method_to_activity_id(std::string_view method) {
    return string_switch<sa::api_activity::activity_id>(method)
      .match_all("POST", "post", sa::api_activity::activity_id::create)
      .match_all("GET", "get", sa::api_activity::activity_id::read)
      .match_all("PUT", "put", sa::api_activity::activity_id::update)
      .match_all("DELETE", "delete", sa::api_activity::activity_id::delete_id)
      .default_match(sa::api_activity::activity_id::unknown);
}

sa::http_request from_ss_http_request(const ss::http::request& req) {
    using ss_http_headers_t = decltype(req._headers);
    const auto get_headers = [](ss_http_headers_t headers) {
        std::vector<sa::http_header> audit_headers;
        std::transform(
          headers.begin(),
          headers.end(),
          std::back_inserter(audit_headers),
          [](const auto& kv) {
              return sa::http_header{.name = kv.first, .value = kv.second};
          });
        return audit_headers;
    };
    return sa::http_request{
        .http_headers = get_headers(req._headers),
        .http_method = req._method,
        .url = {.hostname = "", .path = req.get_url(), .port = sa::port_t{req.get_client_address().port()}, .scheme = req.get_protocol_name(), .url_string = req.get_url()},
        .user_agent = "",
        .version = req._version};
}

sa::network_endpoint from_ss_endpoint(const ss::socket_address& sa) {
    return sa::network_endpoint{
      .addr = net::unresolved_address(
        fmt::format("{}", sa.addr()), sa.port(), sa.addr().in_family())};
}

/// TODO: Via ACLs metadata return correct response
sa::api_activity_unmapped unmapped_data() {
    return sa::api_activity_unmapped{.shard_id = ss::this_shard_id()};
}

sa::api_activity make_api_activity_event_impl(
  const ss::http::request& req, const sa::actor& ator) {
    const auto time_now = sa::timestamp_t{
      std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch())
        .count()};

    return sa::api_activity(
      http_method_to_activity_id(req._method),
      ator,
      sa::api{.operation = req.get_url()},
      from_ss_endpoint(req._client_address),
      from_ss_http_request(req),
      std::vector<sa::resource_detail>(),
      sa::severity_id::informational,
      from_ss_endpoint(req._server_address),
      sa::api_activity::status_id::success,
      time_now,
      unmapped_data());
}

} // namespace

namespace security::audit {

api_activity make_api_activity_event(
  const ss::http::request& req, const authorization_denied& denied) {
    actor ator{
      .authorizations = {
        {.decision = "unauthorized", .policy = policy{.desc = denied.msg}}}};
    return make_api_activity_event_impl(req, ator);
}

api_activity make_api_activity_event(
  const ss::http::request& req, const authorization_allowed& allowed) {
    actor ator{
      .authorizations = {
        {.decision = "authorized",
         .policy = policy{
           .desc = allowed.mechanism, .name = allowed.username}}}};
    return make_api_activity_event_impl(req, ator);
}

} // namespace security::audit
