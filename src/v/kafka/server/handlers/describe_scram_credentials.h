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

#pragma once
#include "kafka/protocol/describe_scram_credentials.h"
#include "kafka/server/handlers/handler.h"

namespace kafka {

using describe_user_scram_credentials_topics_handler
  = single_stage_handler<describe_user_scram_credentials_api, 0, 0>;

}
