# Copyright 2023 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import re
import time
import json
from functools import partial, reduce
from rptest.services.rpk_consumer import RpkConsumer
from ducktape.utils.util import wait_until
from rptest.tests.redpanda_test import RedpandaTest
from rptest.services.cluster import cluster
from rptest.services.admin import Admin
from rptest.clients.rpk import RpkTool
from rptest.tests.cluster_config_test import wait_for_version_sync
from rptest.utils.audit_schemas import validate_audit_schema


class AuditLogTests(RedpandaTest):
    audit_log = "__audit_log"

    def __init__(self, test_context):
        self._extra_conf = {
            'audit_enabled': True,
            'audit_log_num_partitions': 8,
            'audit_enabled_event_types': ['heartbeat'],
        }
        super(AuditLogTests, self).__init__(test_context=test_context,
                                            extra_rp_conf=self._extra_conf)
        self._ctx = test_context
        self.admin = Admin(self.redpanda)
        self.rpk = RpkTool(self.redpanda)

    def _wait_for_audit_log(self, timeout_sec):
        wait_until(lambda: self.audit_log in self.rpk.list_topics(),
                   timeout_sec=timeout_sec,
                   backoff_sec=2)

    def _audit_log_total_number_records(self):
        audit_partitions = self.rpk.describe_topic(self.audit_log)
        partition_totals = [
            partition.high_watermark for partition in audit_partitions
        ]
        assert len(
            partition_totals) == self._extra_conf['audit_log_num_partitions']
        return sum(partition_totals)

    def _read_all_from_audit_log(self,
                                 filter_fn,
                                 stop_cond,
                                 timeout_sec=30,
                                 backoff_sec=1):
        class MessageMapper():
            def __init__(self, logger, filter_fn, stop_cond):
                self.logger = logger
                self.records = []
                self.filter_fn = filter_fn
                self.stop_cond = stop_cond
                self.next_offset_ingest = 0

            def ingest(self, records):
                new_records = records[self.next_offset_ingest:]
                self.next_offset_ingest = len(records)
                new_records = [json.loads(msg['value']) for msg in new_records]
                self.logger.info(f"Ingested: {len(new_records)} records")
                self.records += [r for r in new_records if self.filter_fn(r)]

            def is_finished(self):
                return stop_cond(self.records)

        mapper = MessageMapper(self.redpanda.logger, filter_fn, stop_cond)
        n = self._audit_log_total_number_records()

        consumer = RpkConsumer(self._ctx, self.redpanda, self.audit_log)
        consumer.start()

        def predicate():
            mapper.ingest(consumer.messages)
            return consumer.message_count >= n and mapper.is_finished()

        wait_until(predicate, timeout_sec=timeout_sec, backoff_sec=backoff_sec)
        consumer.stop()
        consumer.free()
        return mapper.records

    @cluster(num_nodes=4)
    def test_audit_log_functioning(self):
        """
        Ensures that the audit log can be produced to when the audit_enabled()
        configuration option is set, and that the same actions do nothing
        when the option is unset. Furthermore verifies that the internal duplicate
        aggregation feature is working.
        """
        def is_api_match(matches, record):
            regex = re.compile(
                "http:\/\/(?P<address>.*):(?P<port>\d+)\/v1\/(?P<handler>.*)")
            match = regex.match(record['api']['operation'])
            if match is None:
                raise RuntimeError('Record out of spec')
            return match.group('handler') in matches

        def number_of_records_matching(filter_by, n_expected):
            predicate = partial(is_api_match, filter_by)

            def aggregate_count(records):
                # Duplicate records are combined with the 'count' field
                def combine(acc, x):
                    return acc + (1 if 'count' not in x else x['count'])

                return reduce(combine, records, 0)

            stop_cond = lambda records: aggregate_count(records) == n_expected
            return self._read_all_from_audit_log(predicate, stop_cond)

        self._wait_for_audit_log(timeout_sec=10)

        # The test override the default event type to 'heartbeat', therefore
        # any actions on the admin server should not result in audit msgs
        api_calls = {
            'features/license': self.admin.get_license,
            'cluster/health_overview': self.admin.get_cluster_health_overview
        }
        api_keys = api_calls.keys()
        call_apis = lambda: [fn() for fn in api_calls.values()]
        for _ in range(0, 500):
            call_apis()

        # Wait 1 second because asserting that 0 records of a type occur
        # will not continuously loop for the records we are expected not to
        # see. 1 second is long enough for the audit fibers to run and push
        # data to the audit topic.
        time.sleep(1)
        _ = number_of_records_matching(api_keys, 0)

        # Enable the management setting which tracks adminAPI events
        patch_result = self.admin.patch_cluster_config(
            upsert={'audit_enabled_event_types': ['management']})
        wait_for_version_sync(self.admin, self.redpanda,
                              patch_result['config_version'])

        for _ in range(0, 500):
            call_apis()
        records = number_of_records_matching(api_keys, 1000)
        self.redpanda.logger.info(f"records: {records}")

        # Raises if validation fails on any records
        [validate_audit_schema(record) for record in records]
