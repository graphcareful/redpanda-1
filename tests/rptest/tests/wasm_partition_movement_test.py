# Copyright 2021 Vectorized, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import random

from rptest.services.admin import Admin

from ducktape.mark.resource import cluster
from ducktape.utils.util import wait_until

from rptest.tests.end_to_end import EndToEndTest
from rptest.clients.rpk import RpkTool

from rptest.wasm.wasm_build_tool import WasmTemplateRepository, WasmBuildTool
from rptest.wasm.topic import construct_materialized_topic
from rptest.wasm.wasm_script import WasmScript

from rptest.tests.partition_movement import PartitionMovementUtils
from rptest.clients.types import TopicSpec


class WasmPartitionMovementTest(EndToEndTest, PartitionMovementUtils):
    """
    Tests to ensure that the materialized partition movement feature
    is working as expected. This feature has materialized topics move to
    where their respective sources are moved to.
    """
    def __init__(self, ctx, *args, **kvargs):
        super(WasmPartitionMovementTest, self).__init__(
            ctx,
            *args,
            extra_rp_conf={
                # Disable leader balancer, as this test is doing its own
                # partition movement and the balancer would interfere
                'enable_leader_balancer': False,
                'enable_coproc': True,
                'developer_mode': True
            },
            **kvargs)
        self._ctx = ctx
        self._rpk_tool = None
        self._build_tool = None

    def start_redpanda_nodes(self, nodes):
        self.start_redpanda(num_nodes=nodes)
        self._rpk_tool = RpkTool(self.redpanda)
        self._build_tool = WasmBuildTool(self._rpk_tool)

    def _deploy_identity_copro(self, inputs, outputs):
        script = WasmScript(inputs=inputs,
                            outputs=outputs,
                            script=WasmTemplateRepository.IDENTITY_TRANSFORM)

        self._build_tool.build_test_artifacts(script)

        self._rpk_tool.wasm_deploy(
            script.get_artifact(self._build_tool.work_dir), script.name,
            "ducktape")

    def _verify_materialized_assignments(self, topic, partition, assignments):
        admin = Admin(self.redpanda)
        massignments = self._get_assignments(admin, topic, partition)
        self.logger.info(
            f"materialized assignments for {topic}-{partition}: {massignments}"
        )

        def status_done():
            info = admin.get_partitions(topic, partition)
            self.logger.info(
                f"current materialized assignments for {topic}-{partition}: {info}"
            )
            self.logger.info(f"Comparing to: {assignments}")
            converged = self._equal_assignments(info["replicas"], assignments)
            return converged and info["status"] == "done"

        # wait until redpanda reports complete
        wait_until(status_done, timeout_sec=90, backoff_sec=2)

        def derived_done():
            info = self._get_current_partitions(admin, topic, partition)
            self.logger.info(
                f"derived materialized assignments for {topic}-{partition}: {info}"
            )
            self.logger.info(f"Comparing to: {assignments}")
            return self._equal_assignments(info, assignments)

        wait_until(derived_done, timeout_sec=90, backoff_sec=2)

    def _grab_input(self, topic):
        metadata = self.redpanda.describe_topics()
        selected = [x for x in metadata if x['topic'] == topic]
        assert len(selected) == 1
        partition = random.choice(selected[0]["partitions"])
        return selected[0]["topic"], partition["partition"]

    @cluster(num_nodes=6)
    def test_dynamic(self):
        """
        Move partitions with active consumer / producer
        """
        output_topic = "identity_output"
        self.start_redpanda_nodes(3)
        spec = TopicSpec(name="topic", partition_count=3, replication_factor=3)
        self.redpanda.create_topic(spec)
        self._deploy_identity_copro([spec.name], [output_topic])
        self.topic = spec.name
        self.start_producer(1)
        self.start_consumer(1)
        self.await_startup(min_records=500)
        materialized_topic = construct_materialized_topic(
            spec.name, output_topic)

        def topic_created():
            metadata = self.redpanda.describe_topics()
            self.logger.info(f"metadata: {metadata}")
            return any([x['topic'] == materialized_topic for x in metadata])

        wait_until(topic_created, timeout_sec=30, backoff_sec=2)

        t, p = self._grab_input(spec.name)
        for _ in range(2):
            _, partition, assignments = self._do_move_and_verify(t, p)
            self._verify_materialized_assignments(materialized_topic,
                                                  partition, assignments)
        self.run_validation(enable_idempotence=False, consumer_timeout_sec=45)

        # Assert all materialized content could be consumed
        consumed_onto_source = self.consumer.total_consumed()
        self.topic = materialized_topic
        self.start_consumer(1)
        self.await_startup(min_records=consumed_onto_source, timeout_sec=30)
        self.consumer.stop()

        assert self.consumer.total_consumed() == consumed_onto_source
