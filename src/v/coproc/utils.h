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

#include "model/fundamental.h"
#include "model/record_batch_reader.h"
#include "raft/types.h"
#include "storage/log.h"
#include "storage/log_reader.h"

#include <optional>
namespace coproc {

struct batch_info {
    model::offset last;
    std::size_t total_size_bytes;
    model::record_batch_reader rbr;
};

ss::future<std::optional<batch_info>>
  extract_batch_info(model::record_batch_reader);

inline storage::log_reader_config
reader_cfg(model::offset start, std::size_t amount, ss::abort_source& as) {
    return storage::log_reader_config(
      start,
      model::model_limits<model::offset>::max(),
      1,
      amount,
      ss::default_priority_class(),
      raft::data_batch_type,
      std::nullopt,
      as);
}

ss::future<bool> write_checked(storage::log log, model::record_batch_reader);

} // namespace coproc
