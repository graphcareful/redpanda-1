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

#include <absl/container/btree_map.h>

#include <algorithm>

namespace kafka {
static describe_user_scram_credentials_result create_scram_credentials_result(
  const security::credential_user& user,
  const security::credential_store::credential_types& types) {
    return std::visit(
      [&user](auto&& arg) {
          using T = std::decay_t<decltype(arg)>;
          if constexpr (std::is_same_v<T, security::scram_credential>) {
              return describe_user_scram_credentials_result{
                .user = user(),
                .error_code = error_code::none,
                .credential_infos = {credential_info{
                  .mechanism = static_cast<
                    std::underlying_type_t<security::credential_type_t>>(
                    arg.type()),
                  .iterations = arg.iterations()}}};
          }
          return describe_user_scram_credentials_result{
            .user = user(),
            .error_code = error_code::resource_not_found,
            .error_message = fmt::format("Unknown credential info: {}", user),
          };
      },
      types);
}

using request_iterator = std::vector<user_name>::iterator;

template<typename ResultIter>
request_iterator validate_range_duplicates(
  request_iterator begin, request_iterator end, ResultIter out_it) {
    absl::flat_hash_map<ss::sstring, uint32_t> freq;
    freq.reserve(std::distance(begin, end));
    for (auto const& r : boost::make_iterator_range(begin, end)) {
        freq[r.name]++;
    }
    auto valid_range_end = std::partition(
      begin, end, [&freq](const user_name& item) {
          return freq[item.name] == 1;
      });
    std::transform(valid_range_end, end, out_it, [](const user_name& item) {
        return describe_user_scram_credentials_result{
          .user = item.name,
          .error_code = error_code::duplicate_resource,
          .error_message = fmt::format(
            "Duplicate resource found: {}", item.name)};
    });
    return valid_range_end;
}

template<typename ResultIter, typename Predicate>
request_iterator validate_range(
  request_iterator begin,
  request_iterator end,
  ResultIter out_it,
  error_code ec,
  const ss::sstring& error_message,
  Predicate&& p) {
    auto valid_range_end = std::partition(
      begin, end, std::forward<Predicate>(p));
    std::transform(
      valid_range_end,
      end,
      out_it,
      [ec, &error_message](const user_name& user) {
          return describe_user_scram_credentials_result{
            .user = user.name,
            .error_code = ec,
            .error_message = error_message,
          };
      });
    return valid_range_end;
}

template<>
ss::future<response_ptr> describe_user_scram_credentials_topics_handler::handle(
  request_context ctx, [[maybe_unused]] ss::smp_service_group g) {
    describe_user_scram_credentials_request request;
    describe_user_scram_credentials_response response{
      .data = describe_user_scram_credentials_response_data{
        .error_code = error_code::none}};
    request.decode(ctx.reader(), ctx.header().version);
    vlog(
      klog.debug,
      "Handling {} request: {}",
      describe_user_scram_credentials_api::name,
      request);

    /// Perform auth check
    const bool cluster_authorized = ctx.authorized(
      security::acl_operation::describe_configs,
      security::default_cluster_name);
    if (!cluster_authorized) {
        response.data.error_code = error_code::cluster_authorization_failed;
        response.data.error_message = "Unauthorized request";
        return ctx.respond(response);
    }

    if (!request.data.users || request.data.users->empty()) {
        /// If the list of users is null/empty, then describe all users
        for (const auto& [user, type] : ctx.credentials()) {
            response.data.results.push_back(
              create_scram_credentials_result(user, type));
        }
    } else {
        /// Otherwise, only describe the users requested
        auto valid_range_end = validate_range_duplicates(
          request.data.users->begin(),
          request.data.users->end(),
          std::back_inserter(response.data.results));

        valid_range_end = validate_range(
          request.data.users->begin(),
          valid_range_end,
          std::back_inserter(response.data.results),
          error_code::resource_not_found,
          "Resource not found",
          [&ctx](const user_name& user) {
              return ctx.credentials().contains(
                security::credential_user(user.name));
          });

        for (auto it = request.data.users->begin(); it != valid_range_end;
             ++it) {
            auto cred_user = security::credential_user(it->name);
            auto types = ctx.credentials().get_types(cred_user);
            vassert(types, "Failed to move response to error batch");
            response.data.results.push_back(
              create_scram_credentials_result(cred_user, *types));
        }
    }
    return ctx.respond(response);
}
} // namespace kafka
