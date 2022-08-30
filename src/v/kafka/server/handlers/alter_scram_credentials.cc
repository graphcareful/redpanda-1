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

#include "alter_scram_credentials.h"

#include "security/scram_algorithm.h"
#include "security/scram_authenticator.h"

namespace kafka {
using opaque_action_t
  = std::variant<scram_credential_deletion, scram_credential_upsertion>;

static std::vector<opaque_action_t>
create_opaque_action_collection(const alter_user_scram_credentials_request& r) {
    std::vector<opaque_action_t> op;
    for (const auto& d : r.data.deletions) {
        op.emplace_back(opaque_action_t(d));
    }
    for (const auto& u : r.data.upsertions) {
        op.emplace_back(opaque_action_t(u));
    }
    return op;
}

static absl::flat_hash_map<ss::sstring, uint32_t>
create_user_cache(const std::vector<opaque_action_t>& op) {
    absl::flat_hash_map<ss::sstring, uint32_t> name_cache;
    for (const auto& d : op) {
        name_cache[std::visit([](const auto& v) { return v.name; }, d)]++;
    }
    return name_cache;
}

static bool credential_iteration_check(int32_t iterations) {
    static const auto min_allowable_iterations = 0;
    static const auto max_allowable_iterations = 16384;
    return iterations >= min_allowable_iterations
           && iterations <= max_allowable_iterations;
}

static bool sasl_mechanism_check(int8_t mechanism) {
    return mechanism == 1 || mechanism == 2;
}

static alter_user_scram_credentials_result map_error_code(std::error_code) {
    return alter_user_scram_credentials_result{};
}

using request_iterator = std::vector<opaque_action_t>::iterator;

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
      [ec, &error_message](const opaque_action_t& user) {
          return alter_user_scram_credentials_result{
            .user = std::visit([](const auto& v) { return v.name; }, user),
            .error_code = ec,
            .error_message = error_message,
          };
      });
    return valid_range_end;
}

static ss::future<std::error_code>
create_user(request_context& ctx, scram_credential_upsertion u) {
    const auto mech_type = security::mechanism_to_credential_type(u.mechanism);
    security::scram_credential credential;
    if (mech_type == security::scram_sha256_authenticator::mech) {
        credential = security::scram_sha256::make_credentials(
          u.salt, u.salted_password, u.iterations);

    } else if (mech_type == security::scram_sha512_authenticator::mech) {
        credential = security::scram_sha512::make_credentials(
          u.salt, u.salted_password, u.iterations);
    } else {
        return ss::make_exception_future<std::error_code>(
          make_error_code(error_code::none));
    }

    return ctx.security_frontend().create_user(
      security::credential_user(u.name),
      credential,
      model::timeout_clock::now() + 5s);
}

template<>
ss::future<response_ptr> alter_user_scram_credentials_topics_handler::handle(
  request_context ctx, [[maybe_unused]] ss::smp_service_group g) {
    alter_user_scram_credentials_request request;
    alter_user_scram_credentials_response response;
    request.decode(ctx.reader(), ctx.header().version);
    vlog(
      klog.debug,
      "Handling {} request: {}",
      alter_user_scram_credentials_api::name,
      request);

    auto actions = create_opaque_action_collection(request);

    /// Perform auth check
    const bool cluster_authorized = ctx.authorized(
      security::acl_operation::alter, security::default_cluster_name);

    if (!cluster_authorized) {
        validate_range(
          actions.begin(),
          actions.end(),
          std::back_inserter(response.data.results),
          error_code::cluster_authorization_failed,
          "Unauthorized",
          [&cluster_authorized](const opaque_action_t&) {
              return cluster_authorized;
          });
        co_return co_await ctx.respond(response);
    }

    const auto user_cache = create_user_cache(actions);
    auto valid_range_end = validate_range(
      actions.begin(),
      actions.end(),
      std::back_inserter(response.data.results),
      error_code::duplicate_resource,
      "Duplicate resource",
      [&user_cache](const opaque_action_t& opt) {
          const auto it = user_cache.find(
            std::visit([](const auto& v) { return v.name; }, opt));
          return it->second > 1;
      });

    valid_range_end = validate_range(
      actions.begin(),
      valid_range_end,
      std::back_inserter(response.data.results),
      error_code::unacceptable_credential,
      "Unacceptable credential",
      [](const opaque_action_t& opt) {
          return std::visit(
            [&](const auto& v) {
                using T = std::decay_t<decltype(v)>;
                if constexpr (std::is_same_v<T, scram_credential_upsertion>) {
                    if (!credential_iteration_check(v.iterations)) {
                        return false;
                    }
                }
                return v.name != "";
            },
            opt);
      });

    valid_range_end = validate_range(
      actions.begin(),
      valid_range_end,
      std::back_inserter(response.data.results),
      error_code::unsupported_sasl_mechanism,
      "Unsupported sasl mechanism",
      [](const opaque_action_t& opt) {
          return std::visit(
            [](const auto& v) { return sasl_mechanism_check(v.mechanism); },
            opt);
      });

    valid_range_end = validate_range(
      actions.begin(),
      valid_range_end,
      std::back_inserter(response.data.results),
      error_code::resource_not_found,
      "Resource not found",
      [&ctx](const opaque_action_t& opt) {
          return std::visit(
            [&ctx](const auto& v) {
                using T = std::decay_t<decltype(v)>;
                if constexpr (std::is_same_v<T, scram_credential_deletion>) {
                    return ctx.credentials().contains(
                      security::credential_user(v.name));
                }
                return true;
            },
            opt);
      });

    for (auto it = actions.begin(); it != actions.end(); ++it) {
        auto f = std::visit(
          [&](const auto& v) -> ss::future<> {
              using T = std::decay_t<decltype(v)>;
              std::error_code ec;
              if constexpr (std::is_same_v<T, scram_credential_deletion>) {
                  ec = co_await ctx.security_frontend().delete_user(
                    security::credential_user(v.name),
                    model::timeout_clock::now() + 5s);
              } else if constexpr (std::
                                     is_same_v<T, scram_credential_upsertion>) {
                  ec = co_await create_user(ctx, v);
              }
              response.data.results.push_back(map_error_code(ec));
          },
          *it);
        co_await std::move(f);
    }
    co_return co_await ctx.respond(response);
}
} // namespace kafka
