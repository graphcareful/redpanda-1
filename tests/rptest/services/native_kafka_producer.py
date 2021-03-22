# Copyright 2021 Vectorized, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import os

from ducktape.services.background_thread import BackgroundThreadService
from kafka import KafkaProducer


class NativeKafkaProducer(BackgroundThreadService):
    def __init__(self,
                 context,
                 redpanda,
                 topic,
                 num_records,
                 records_size=4192):
        super(NativeKafkaProducer, self).__init__(context, num_nodes=1)
        self._redpanda = redpanda
        self._topic = topic
        self._num_records = num_records
        self._records_size = records_size
        try:
            self._producer = KafkaProducer(
                bootstrap_servers=self._redpanda.brokers(),
                key_serializer=str.encode)
        except Exception as e:
            self.logger.error(f"Failed to create KafkaProducer: {e}")
            raise

    def _worker(self, idx, node):
        for i in range(0, self._num_records):
            key = str(hash(str(i) + self._topic))
            value = os.urandom(self._records_size)
            self._producer.send(self._topic, key=key, value=value)

    def stop_node(self, node):
        self._producer.flush()
        self._producer.stop()
