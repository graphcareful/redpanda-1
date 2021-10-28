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

#include "cluster/partition.h"
#include "storage/log.h"

#include <seastar/core/gate.hh>

namespace cluster {

class non_replicable_partition {
public:
    non_replicable_partition(
      storage::log log, ss::lw_shared_ptr<partition> source) noexcept
      : _log(log)
      , _source(source) {}

    ss::future<> start() { return ss::now(); }
    ss::future<> stop() { return _gate.close(); }

    const model::ntp& source() const { return _source->ntp(); }
    const model::ntp& ntp() const { return _log.config().ntp(); }
    const storage::ntp_config& config() const { return _log.config(); }
    bool is_leader() const { return _source->is_leader(); }
    model::revision_id get_revision_id() const {
        return _log.config().get_revision();
    }

    ss::future<model::record_batch_reader>
    make_reader(storage::log_reader_config config) {
        class guided_shutdown_reader final
          : public model::record_batch_reader::impl {
        public:
            guided_shutdown_reader(
              ss::gate& g,
              std::unique_ptr<model::record_batch_reader::impl> impl)
              : _gate(g)
              , _impl(std::move(impl)) {}

            bool is_end_of_stream() const final {
                return _gate.is_closed() || _impl->is_end_of_stream();
            }

            ss::future<model::record_batch_reader::storage_t>
            do_load_slice(model::timeout_clock::time_point t) final {
                auto holder = _gate.hold();
                return _impl->do_load_slice(t).finally([holder] {});
            }

            void print(std::ostream& os) final {
                os << "guided_shutdown_reader";
            }

        private:
            ss::gate& _gate;
            std::unique_ptr<model::record_batch_reader::impl> _impl;
        };
        return _log.make_reader(config).then(
          [this](model::record_batch_reader rdr) {
              return model::make_record_batch_reader<guided_shutdown_reader>(
                _gate, std::move(rdr).release());
          });
    }

    // ss::future<std::optional<storage::timequery_result>>
    // timequery(model::timestamp ts, ss::io_priority_class pc) final {
    //     auto holder = _gate.hold();
    //     storage::timequery_config cfg(ts, _log.offsets().dirty_offset,
    //     io_pc); return _log.timequery(cfg);
    // };

private:
    ss::gate _gate;
    storage::log _log;
    ss::lw_shared_ptr<partition> _source;
};

} // namespace cluster
