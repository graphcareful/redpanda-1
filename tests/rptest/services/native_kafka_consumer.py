# Copyright 2021 Vectorized, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import time
import uuid

from ducktape.services.background_thread import BackgroundThreadService
from kafka import KafkaConsumer
from rptest.wasm.topics_result_set import TopicsResultSet


class NativeKafkaConsumer(BackgroundThreadService):
    def __init__(self, context, redpanda, topic_partitions, num_records=1):
        super(NativeKafkaConsumer, self).__init__(context, num_nodes=1)
        self._redpanda = redpanda
        self._topic_partitions = topic_partitions
        self._num_records = num_records
        self.done = False
        self.results = TopicsResultSet()
        try:
            self._consumer = KafkaConsumer(
                client_id=uuid.uuid4(),
                bootstrap_servers=self._redpanda.brokers(),
                request_timeout_ms=1000,
                enable_auto_commit=False,
                auto_offset_reset="earliest")
        except Exception as e:
            self.logger.error(f"Failed to create KafkaConsumer: {e}")
            raise

    def _worker(self, idx, node):
        def stop_consume(empty_iterations):
            read_all = self.results.num_records() >= self._num_records
            waited_enough = empty_iterations <= 0
            return self.done or read_all or waited_enough

        self._consumer.assign(self._topic_partitions)
        empty_iterations = 10
        try:
            while not stop_consume(empty_iterations):
                # max_records is the largest chunk to be returned by a single
                # call to 'poll.
                r = self._consumer.poll(timeout_ms=100, max_records=4092)
                if len(r) == 0:
                    empty_iterations -= 1
                    time.sleep(1)
                else:
                    self.results.append(r)
        except Exception as e:
            self.logger.error(f"Failed during consumption with error {e}")
            raise
        finally:
            self.done = True

    def stop_node(self, node):
        # TODO: Stop is called before start, then after thread shuts down
        # self.done = True
        self._consumer.stop()
