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

#include "coproc/utils.h"

#include "coproc/reference_window_consumer.hpp"
namespace coproc {

ss::future<std::optional<batch_info>>
extract_batch_info(model::record_batch_reader rbr) {
    return model::consume_reader_to_memory(std::move(rbr), model::no_timeout)
      .then(
        [](model::record_batch_reader::data_t data)
          -> std::optional<batch_info> {
            if (data.empty()) {
                /// TODO(rob) Its possible to recieve an empty batch when a
                /// batch type filter is enabled on the reader which for copro
                /// reads, is enabled. Why does this happen? Not sure, would be
                /// nice to just have the reader return the correct batches
                /// after the filtered type
                return std::nullopt;
            }
            const model::offset last_offset = data.back().last_offset();
            const std::size_t total_size = std::accumulate(
              data.cbegin(),
              data.cend(),
              std::size_t(0),
              [](std::size_t acc, const model::record_batch& x) {
                  return acc + x.size_bytes();
              });
            return batch_info{
              .last = last_offset,
              .total_size_bytes = total_size,
              .rbr = model::make_memory_record_batch_reader(std::move(data))};
        });
}

ss::future<bool>
write_checked(storage::log log, model::record_batch_reader reader) {
    static const storage::log_append_config write_cfg{
      .should_fsync = storage::log_append_config::fsync::no,
      .io_priority = ss::default_priority_class(),
      .timeout = model::no_timeout};
    return std::move(reader)
      .for_each_ref(
        coproc::reference_window_consumer(
          model::record_batch_crc_checker(), log.make_appender(write_cfg)),
        model::no_timeout)
      .then([](std::tuple<bool, ss::future<storage::append_result>> t) mutable {
          const auto& [crc_parse_success, _] = t;
          return !crc_parse_success;
      });
}

} // namespace coproc
