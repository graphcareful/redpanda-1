# Copyright 2021 Vectorized, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from rptest.wasm.wasm_test import WasmTest, WasmScript
from rptest.wasm.wasm_build_tool import WasmTemplateRepository
from rptest.wasm.topic import construct_materialized_topic
from rptest.wasm.topics_result_set import materialized_result_set_compare

from rptest.clients.types import TopicSpec


class WasmBasicTest(WasmTest):
    topics = (TopicSpec(name="myinputtopic",
                        partition_count=3,
                        replication_factor=3,
                        cleanup_policy=TopicSpec.CLEANUP_DELETE), )

    def __init__(self, test_context, num_records=1024, record_size=1024):
        super(WasmBasicTest, self).__init__(test_context, extra_rp_conf={})
        self._num_records = num_records
        self._record_size = record_size
        assert len(self.topics) >= 1

    def wasm_test_plan(self):
        input_topic = self.topics[0].name
        mapped_topic = "myoutputtopic"
        output_topic = construct_materialized_topic(input_topic, mapped_topic)

        # The identity transform produces 1 identital record onto a topic for
        # each input record. The result should be a 1 to 1 mapping between a
        # source and destination topic, they should be identical when compared
        basic_script = WasmScript(
            inputs=[(input_topic, (self._num_records, self._record_size))],
            outputs=[(output_topic, self._num_records)],
            script=WasmTemplateRepository.IDENTITY_TRANSFORM)

        return [basic_script]

    def assert_test_results(self, input_results, output_results):
        # Read all of the data from the materialized topic, expecting the same
        # number of records which were produced onto the input topic
        assert input_results.num_records() == self._num_records
        if not materialized_result_set_compare(input_results, output_results):
            raise Exception(
                "Expected all records across topics to be equivalent")


class WasmLargeDataSetTest(WasmBasicTest):
    topics = (TopicSpec(name="biginputtopic",
                        partition_count=3,
                        replication_factor=3,
                        cleanup_policy=TopicSpec.CLEANUP_DELETE), )

    def __init__(self, test_context):
        # 250,000 * 1024b ~ 1/4GB test
        super(WasmLargeDataSetTest, self).__init__(test_context,
                                                   num_records=250000,
                                                   record_size=1024)

    def wasm_test_timeout(self):
        return (300, 3)
