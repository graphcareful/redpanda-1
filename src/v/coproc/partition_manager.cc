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

#include "coproc/partition_manager.h"

#include "cluster/logger.h"
#include "cluster/partition.h"
#include "coproc/logger.h"
#include "storage/api.h"
#include "vlog.h"

#include <seastar/core/coroutine.hh>

namespace coproc {

partition_manager::partition_manager(
  ss::sharded<storage::api>& storage) noexcept
  : _storage(storage.local()) {}

ss::lw_shared_ptr<partition>
partition_manager::get(const model::ntp& ntp) const {
    if (auto it = _ntp_table.find(ntp); it != _ntp_table.end()) {
        return it->second;
    }
    return nullptr;
}

ss::future<> partition_manager::manage(
  storage::ntp_config ntp_cfg, ss::lw_shared_ptr<cluster::partition> src) {
    auto holder = _gate.hold();
    storage::log log = co_await _storage.log_mgr().manage(std::move(ntp_cfg));
    vlog(
      coproclog.info,
      "Non replicable log created manage completed, ntp: {}, rev: {}, {} "
      "segments, {} bytes",
      log.config().ntp(),
      log.config().get_revision(),
      log.segment_count(),
      log.size_bytes());

    auto nrp = ss::make_lw_shared<partition>(log, src);
    _ntp_table.emplace(log.config().ntp(), nrp);
    co_await nrp->start();
}

ss::future<> partition_manager::stop_partitions() {
    co_await _gate.close();
    auto partitions = std::exchange(_ntp_table, {});
    co_await ss::parallel_for_each(
      partitions, [this](ntp_table_container::value_type& e) {
          return do_shutdown(e.second);
      });
}

ss::future<> partition_manager::remove(const model::ntp& ntp) {
    auto partition = get(ntp);

    if (!partition) {
        throw std::invalid_argument(fmt_with_ctx(
          ssx::sformat,
          "Can not remove non_replicable partition. NTP {} is not "
          "present in partition manager: ",
          ntp));
    }

    _ntp_table.erase(ntp);
    co_await partition->stop();
    co_await _storage.log_mgr().remove(ntp);
}

ss::future<>
partition_manager::do_shutdown(ss::lw_shared_ptr<partition> partition) {
    try {
        auto ntp = partition->ntp();
        co_await partition->stop();
        co_await _storage.log_mgr().shutdown(partition->ntp());
    } catch (...) {
        vassert(
          false,
          "error shutting down non replicable partition {},  "
          "non_replicable partition manager state: {}, error: {} - "
          "terminating redpanda",
          partition->ntp(),
          *this,
          std::current_exception());
    }
}

std::ostream& operator<<(std::ostream& os, const partition_manager& nr_pm) {
    fmt::print(
      os,
      "{shard: {}, mngr: {}, ntp_table.size(), {}}",
      ss::this_shard_id(),
      nr_pm._storage.log_mgr(),
      nr_pm._ntp_table.size());
    return os;
}

} // namespace coproc
