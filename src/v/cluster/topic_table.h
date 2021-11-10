/*
 * Copyright 2020 Vectorized, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "cluster/commands.h"
#include "cluster/types.h"
#include "model/fundamental.h"
#include "utils/expiring_promise.h"

#include <absl/container/flat_hash_map.h>
#include <absl/container/flat_hash_set.h>

namespace cluster {

/// Topic table represent all topics configuration and partition assignments.
/// The topic table is copied on each core to minimize cross core communication
/// when requesting topic information. The topics table provides an API for
/// Kafka requests and delta API for controller backend. The delta API allows
/// backend to wait for changes in topics table. Topics table is update directly
/// from controller_stm. The table is always updated before any actions related
/// with topic creation or deletion are executed. Topic table is also
/// responsible for commiting or removing pending allocations
///

class topic_table {
public:
    using delta = topic_table_delta;
    using non_rep_delta = non_replicable_topic_table_delta;
    class topic_metadata {
    public:
        topic_metadata(
          topic_configuration_assignment, model::revision_id) noexcept;
        topic_metadata(topic_configuration_assignment, model::topic) noexcept;

        bool is_topic_replicable() const;
        model::revision_id get_revision() const;
        const model::topic& get_source_topic() const;
        const topic_configuration_assignment& get_configuration() const;

    private:
        friend class topic_table;
        topic_configuration_assignment configuration;
        std::variant<model::revision_id, model::topic> _id_or_topic;
    };
    using underlying_t = absl::flat_hash_map<
      model::topic_namespace,
      topic_metadata,
      model::topic_namespace_hash,
      model::topic_namespace_eq>;
    using hierarchy_t = absl::node_hash_map<
      model::topic_namespace,
      absl::flat_hash_set<model::topic_namespace>,
      model::topic_namespace_hash,
      model::topic_namespace_eq>;

    template<typename delta_type>
    using delta_cb_t
      = ss::noncopyable_function<void(const std::vector<delta_type>&)>;

    cluster::notification_id_type
      register_delta_notification(delta_cb_t<delta>);

    void unregister_delta_notification(cluster::notification_id_type);

    bool is_batch_applicable(const model::record_batch& b) const {
        return b.header().type
               == model::record_batch_type::topic_management_cmd;
    }

    // list of commands that this table is able to apply, the list is used to
    // automatically deserialize batch into command
    static constexpr auto accepted_commands = make_commands_list<
      create_topic_cmd,
      delete_topic_cmd,
      move_partition_replicas_cmd,
      finish_moving_partition_replicas_cmd,
      update_topic_properties_cmd,
      create_partition_cmd,
      create_non_replicable_topic_cmd>{};

    /// State machine applies
    ss::future<std::error_code> apply(create_topic_cmd, model::offset);
    ss::future<std::error_code> apply(delete_topic_cmd, model::offset);
    ss::future<std::error_code>
      apply(move_partition_replicas_cmd, model::offset);
    ss::future<std::error_code>
      apply(finish_moving_partition_replicas_cmd, model::offset);
    ss::future<std::error_code>
      apply(update_topic_properties_cmd, model::offset);
    ss::future<std::error_code> apply(create_partition_cmd, model::offset);
    ss::future<std::error_code>
      apply(create_non_replicable_topic_cmd, model::offset);
    ss::future<> stop();

    /// Delta API

    ss::future<std::vector<delta>> wait_for_changes(ss::abort_source& as);
    ss::future<std::vector<non_rep_delta>>
    wait_for_non_rep_changes(ss::abort_source& as);

    bool has_pending_changes() const;
    bool has_non_rep_pending_changes() const;

    /// Query API

    /// Returns list of all topics that exists in the cluster.
    std::vector<model::topic_namespace> all_topics() const;

    ///\brief Returns metadata of single topic.
    ///
    /// If topic does not exists it returns an empty optional
    std::optional<model::topic_metadata>
      get_topic_metadata(model::topic_namespace_view) const;

    ///\brief Returns configuration of single topic.
    ///
    /// If topic does not exists it returns an empty optional
    std::optional<topic_configuration>
      get_topic_cfg(model::topic_namespace_view) const;

    ///\brief Returns topics timestamp type
    ///
    /// If topic does not exists it returns an empty optional
    std::optional<model::timestamp_type>
      get_topic_timestamp_type(model::topic_namespace_view) const;

    /// Returns metadata of all topics.
    std::vector<model::topic_metadata> all_topics_metadata() const;

    /// Checks if it has given partition
    bool contains(model::topic_namespace_view, model::partition_id) const;

    /// Updates partition leader and notify waiters if needed
    void update_partition_leader(
      const model::ntp&, model::term_id, std::optional<model::node_id>);

    std::optional<partition_assignment>
    get_partition_assignment(const model::ntp&) const;

    const underlying_t& topics_map() const { return _topics; }

    bool is_update_in_progress(const model::ntp&) const;

private:
    template<typename delta_type>
    class wait_notification {
    public:
        struct waiter {
            explicit waiter(uint64_t id)
              : id(id) {}
            ss::promise<std::vector<delta_type>> promise;
            ss::abort_source::subscription sub;
            uint64_t id;
        };

        template<typename... Args>
        void emplace_deltas(Args&&... args) {
            _pending_deltas.emplace_back(std::forward<Args>(args)...);
        }

        bool has_pending_changes() const { return !_pending_deltas.empty(); }

        void notify_waiters();

        ss::future<std::vector<delta_type>>
        wait_for_changes(ss::abort_source& as);

        ss::future<> stop();

        cluster::notification_id_type
        register_delta_notification(delta_cb_t<delta_type> cb);
        void unregister_delta_notification(cluster::notification_id_type id);

    private:
        uint64_t _waiter_id{0};
        std::vector<delta_type> _pending_deltas;
        std::vector<std::unique_ptr<waiter>> _waiters;

        cluster::notification_id_type _notification_id{0};
        std::vector<
          std::pair<cluster::notification_id_type, delta_cb_t<delta_type>>>
          _notifications;
    };

    void deallocate_topic_partitions(const std::vector<partition_assignment>&);

    template<typename Func>
    std::vector<std::invoke_result_t<Func, topic_configuration_assignment>>
    transform_topics(Func&&) const;

    underlying_t _topics;
    hierarchy_t _topics_hierarchy;

    absl::flat_hash_set<model::ntp> _update_in_progress;

    wait_notification<delta> _updates;
    wait_notification<non_rep_delta> _non_rep_updates;
};
} // namespace cluster
