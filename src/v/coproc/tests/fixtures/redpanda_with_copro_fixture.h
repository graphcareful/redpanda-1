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

#include "redpanda/tests/fixture.h"

#include <seastar/core/smp.hh>

class enable_coproc {
public:
    enable_coproc() {
        ss::smp::invoke_on_all([]() {
            auto& config = config::shard_local_cfg();
            config.get("enable_coproc").set_value(true);
        }).get0();
    }
};

/// Trick to ensure enable_copro is set to true before the constructor of \ref
/// redpanda_thread_fixture is called
class redpanda_with_copro_fixture
  : public enable_coproc
  , public redpanda_thread_fixture {
public:
    redpanda_with_copro_fixture()
      : enable_coproc()
      , redpanda_thread_fixture() {}
};
