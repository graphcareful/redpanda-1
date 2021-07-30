/*
 * Copyright 2021 Vectorized, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/vectorizedio/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

#include "bytes/iobuf.h"
#include "coproc/types.h"
#include "coproc/wasm_event.h"

#include <seastar/core/coroutine.hh>

#include <absl/container/btree_map.h>
#include <absl/container/flat_hash_map.h>

namespace coproc {

namespace wasm {
struct script {
    iobuf source_code;
    ss::sstring description;
    ss::sstring name;
};

} // namespace wasm

enum class engine_type { winline, sidecar };

class engine {
public:
    virtual ss::future<> deploy_scripts(std::vector<ss::lw_shared_ptr<wasm::script>>) = 0;
    virtual ss::future<> remove_scripts(std::vector<script_id>) = 0;
    virtual ss::future<>
      process_batch(std::vector<ss::lw_shared_ptr<wasm::script>>, model::record_batch_reader) = 0;
};

class js_sidecar_engine : public engine {
public:
    ss::future<result<std::vector<script_id>>> deploy_scripts(std::vector<ss::lw_shared_ptr<wasm::script>>) override {

    }
    ss::future<> remove_scripts(std::vector<script_id>) = 0;
    ss::future<>
      process_batch(std::vector<ss::lw_shared_ptr<wasm::script>>, model::record_batch_reader) = 0;

}

class script_registry {
public:
    template<typename... Args>
    void create_engine(engine_type e, Args&&... args) {
        auto itr = _engines.emplace_back(e, std::forward<Args>(args)...);
        (void)itr->second.start();
    }

    ss::future<> shutdown_engine(engine_type e) {
        auto found = _engines.find(e);
        if (found != _engines.end()) {
            co_await found->second->stop();
            _engines.erase(found);
        }
    }

    ss::future<>
    process_events(absl::btree_map<script_id, wasm::log_event> events) {
        std::vector<script_id> removals;
        std::vector<ss::lw_shared_ptr<wasm::log_event>> additions;
        for (auto& event : events) {
            const auto id = event.first;
            auto found = _registry.find(id);
            if (event.second.is_remove()) {
                if (found != _registry.end()) {
                    removals.push_back(id);
                }
            } else if (found == _registry.end()) {
                additions.push_back(ss::make_lw_shared<wasm::script>(
                  std::move(event)));
            }
        }
        /// Ids are non-overlapping
        return when_all_succeed(remove_scripts(std::move(removals), add_scripts(std::move(additions)));
    }

    ss::lw_shared_ptr<wasm::script> get_script(script_id id) const {
        auto found = _registry.find(id);
        return found == _registry.end() ? nullptr : found->second;
    }

private:
    using underlying_t
      = absl::flat_hash_map<script_id, ss::lw_shared_ptr<wasm::script>>;

    using wasm_engines_t = absl::flat_hash_map<engine_type, engine_protocol*>;

    ss::future<bool> add_script(std::vector<ss::lw_shared_ptr<wasm::script>> scripts) {
        for (auto& engine : _engines) {
            auto deployed = co_await engine.second->deploy_scripts(scripts);
            for(auto& id : deployed) {
                _registry.push_back(id);
            }

        }
        // TODO: at least one added
        co_return true;
    }

    ss::future<> remove_scripts(std::vector<script_id> ids) {
        for (auto& engine : _engines) {
            co_await engine.second->remove_scripts(ids);
        }
        for(auto& id : ids) {
            _registry.erase(id);
        }
    }

    wasm_engines_t _engines;
    underlying_t _registry;
};
} // namespace coproc
