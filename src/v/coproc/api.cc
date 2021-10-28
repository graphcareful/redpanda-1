/*
 * Copyright 2021 Vectorized, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "coproc/api.h"

#include "cluster/non_replicable_topics_frontend.h"
#include "cluster/shard_table.h"
#include "coproc/event_listener.h"
#include "coproc/pacemaker.h"
#include "coproc/reconciliation_backend.h"
#include "coproc/script_database.h"

#include <seastar/core/coroutine.hh>

namespace coproc {

api::api(
  unresolved_address addr,
  ss::sharded<cluster::topic_table>& topic_table,
  ss::sharded<cluster::shard_table>& shard_table,
  ss::sharded<cluster::topics_frontend>& topics_frontend,
  ss::sharded<cluster::metadata_cache>& metadata_cache,
  ss::sharded<cluster::partition_manager>& partition_manager,
  ss::sharded<cluster::non_replicable_partition_manager>&
    nr_partition_manager) noexcept
  : _engine_addr(std::move(addr))
  , _rs(sys_refs{
      .mt_frontend = _mt_frontend,
      .topics_frontend = topics_frontend,
      .metadata_cache = metadata_cache,
      .partition_manager = partition_manager,
      .nr_partition_manager = nr_partition_manager})
  , _topics(topic_table)
  , _shard_table(shard_table) {}

api::~api() = default;

ss::future<> api::start() {
    co_await _sdb.start_single();
    co_await _mt_frontend.start_single(std::ref(_rs.topics_frontend));
    co_await _pacemaker.start(_engine_addr, std::ref(_sdb), std::ref(_rs));
    co_await _pacemaker.invoke_on_all(&coproc::pacemaker::start);
    _listener = std::make_unique<wasm::event_listener>(_as);
    _dispatcher = std::make_unique<wasm::script_dispatcher>(
      _pacemaker, _sdb, _as);
    _wasm_async_handler = std::make_unique<coproc::wasm::async_event_handler>(
      std::ref(*_dispatcher));
    _listener->register_handler(
      coproc::wasm::event_type::async, _wasm_async_handler.get());

    co_await _backend.start(
      std::ref(_sdb),
      std::ref(_rs.storage),
      std::ref(_topics),
      std::ref(_shard_table),
      std::ref(_pacemaker));
    co_await _backend.invoke_on_all(&coproc::reconciliation_backend::start);
    co_await _listener->start();
}

ss::future<> api::stop() {
    auto f = ss::now();
    if (_listener) {
        f = _listener->stop();
    }
    return f.then([this] { return _backend.stop(); })
      .then([this] { return _pacemaker.stop(); })
      .then([this] { return _mt_frontend.stop(); })
      .then([this] { return _sdb.stop(); });
}

} // namespace coproc
