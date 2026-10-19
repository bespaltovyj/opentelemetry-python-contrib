# Copyright The OpenTelemetry Authors
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import asyncio
from typing import Any, cast
from unittest import IsolatedAsyncioTestCase, mock

from aiokafka import (
    AIOKafkaConsumer,
    AIOKafkaProducer,
    ConsumerRecord,
    TopicPartition,
)
from aiokafka.errors import KafkaTimeoutError
from aiokafka.structs import RecordMetadata

from opentelemetry.instrumentation._semconv import (
    OTEL_SEMCONV_STABILITY_OPT_IN,
    _OpenTelemetrySemanticConventionStability,
)
from opentelemetry.instrumentation.aiokafka import AIOKafkaInstrumentor
from opentelemetry.instrumentation.aiokafka._metrics import (
    MESSAGING_KAFKA_CONSUMER_LAG,
)
from opentelemetry.sdk.metrics.export import (
    HistogramDataPoint,
    NumberDataPoint,
)
from opentelemetry.semconv._incubating.attributes.messaging_attributes import (
    MESSAGING_CLIENT_ID,
    MESSAGING_CONSUMER_GROUP_NAME,
    MESSAGING_DESTINATION_NAME,
    MESSAGING_DESTINATION_PARTITION_ID,
    MESSAGING_OPERATION_NAME,
    MESSAGING_OPERATION_TYPE,
    MESSAGING_SYSTEM,
)
from opentelemetry.semconv._incubating.metrics.messaging_metrics import (
    MESSAGING_CLIENT_CONSUMED_MESSAGES,
    MESSAGING_CLIENT_OPERATION_DURATION,
    MESSAGING_CLIENT_SENT_MESSAGES,
)
from opentelemetry.semconv.attributes.error_attributes import ERROR_TYPE
from opentelemetry.test.test_base import TestBase

CLIENT_ID = "client"
GROUP_ID = "group"
TOPIC = "topic_1"


def _record(topic: str, partition: int, offset: int) -> ConsumerRecord:
    return ConsumerRecord(topic, partition, offset, 0, 0, b"key", b"value", None, 3, 5, headers=())


async def _consumer_factory(**consumer_kwargs: Any) -> AIOKafkaConsumer:
    consumer = AIOKafkaConsumer(**consumer_kwargs)
    consumer._client.bootstrap = mock.AsyncMock()
    consumer._client._wait_on_metadata = mock.AsyncMock()
    await consumer.start()
    consumer._fetcher.next_record = mock.AsyncMock()
    consumer._fetcher.fetched_records = mock.AsyncMock()
    return consumer


async def _producer_factory() -> AIOKafkaProducer:
    producer = AIOKafkaProducer()
    producer.client._wait_on_metadata = mock.AsyncMock()
    producer.client.bootstrap = mock.AsyncMock()
    producer._message_accumulator.add_message = mock.AsyncMock()
    producer._sender.start = mock.AsyncMock()
    producer._partition = mock.Mock(return_value=1)
    await producer.start()
    return producer


class _MetricsTestBase(TestBase, IsolatedAsyncioTestCase):
    opt_in = "messaging"

    def setUp(self):
        super().setUp()
        self._env_patch = mock.patch.dict("os.environ", {OTEL_SEMCONV_STABILITY_OPT_IN: self.opt_in})
        self._env_patch.start()
        _OpenTelemetrySemanticConventionStability._initialized = False
        AIOKafkaInstrumentor().instrument(
            tracer_provider=self.tracer_provider,
            meter_provider=self.meter_provider,
        )

    def tearDown(self):
        super().tearDown()
        AIOKafkaInstrumentor().uninstrument()
        self._env_patch.stop()
        _OpenTelemetrySemanticConventionStability._initialized = False

    def points(self, name: str) -> list[Any]:
        metrics_data = self.memory_metrics_reader.get_metrics_data()
        if metrics_data is None:
            return []
        return [
            point
            for resource_metrics in metrics_data.resource_metrics
            for scope_metrics in resource_metrics.scope_metrics
            for metric in scope_metrics.metrics
            if metric.name == name
            for point in metric.data.data_points
        ]

    def assert_point(self, name: str, attributes: dict[str, Any], value: int) -> None:
        points = [point for point in self.points(name) if dict(point.attributes) == attributes]
        self.assertEqual(len(points), 1, self.points(name))
        point = points[0]
        for attribute in point.attributes.values():
            self.assertIsInstance(attribute, str)
        if isinstance(point, HistogramDataPoint):
            self.assertEqual(point.count, value)
            self.assertGreaterEqual(point.sum, 0)
        else:
            self.assertIsInstance(point, NumberDataPoint)
            self.assertEqual(point.value, value)


class TestAIOKafkaProducerMetrics(_MetricsTestBase):
    SEND_ATTRIBUTES = {
        MESSAGING_SYSTEM: "kafka",
        MESSAGING_OPERATION_NAME: "send",
        MESSAGING_DESTINATION_NAME: TOPIC,
        MESSAGING_DESTINATION_PARTITION_ID: "1",
    }

    async def _send(self, outcome: Any) -> asyncio.Future:
        producer = await _producer_factory()
        self.addAsyncCleanup(producer.stop)
        future = asyncio.get_running_loop().create_future()
        cast(mock.AsyncMock, producer._message_accumulator.add_message).return_value = future
        returned = await producer.send(TOPIC, b"value")
        self.assertIs(returned, future)
        # nothing is recorded until the broker acknowledged the message
        self.assertEqual(self.points(MESSAGING_CLIENT_SENT_MESSAGES), [])
        if isinstance(outcome, BaseException):
            future.set_exception(outcome)
        else:
            future.set_result(outcome)
        await asyncio.sleep(0)
        return future

    async def test_send(self):
        await self._send(RecordMetadata(TOPIC, 1, TopicPartition(TOPIC, 1), 10, 0, 0, 0))
        self.assert_point(MESSAGING_CLIENT_SENT_MESSAGES, self.SEND_ATTRIBUTES, 1)
        self.assert_point(
            MESSAGING_CLIENT_OPERATION_DURATION,
            {**self.SEND_ATTRIBUTES, MESSAGING_OPERATION_TYPE: "send"},
            1,
        )

    async def test_send_error(self):
        future = await self._send(KafkaTimeoutError())
        with self.assertRaises(KafkaTimeoutError):
            await future
        attributes = {**self.SEND_ATTRIBUTES, ERROR_TYPE: "KafkaTimeoutError"}
        self.assert_point(MESSAGING_CLIENT_SENT_MESSAGES, attributes, 1)
        self.assert_point(
            MESSAGING_CLIENT_OPERATION_DURATION,
            {**attributes, MESSAGING_OPERATION_TYPE: "send"},
            1,
        )

    async def test_send_raises(self):
        producer = await _producer_factory()
        self.addAsyncCleanup(producer.stop)
        cast(mock.AsyncMock, producer._message_accumulator.add_message).side_effect = KafkaTimeoutError()
        with self.assertRaises(KafkaTimeoutError):
            await producer.send(TOPIC, b"value")
        self.assert_point(
            MESSAGING_CLIENT_SENT_MESSAGES,
            {**self.SEND_ATTRIBUTES, ERROR_TYPE: "KafkaTimeoutError"},
            1,
        )


class TestAIOKafkaConsumerMetrics(_MetricsTestBase):
    RECEIVE_ATTRIBUTES = {
        MESSAGING_SYSTEM: "kafka",
        MESSAGING_OPERATION_NAME: "receive",
        MESSAGING_CONSUMER_GROUP_NAME: GROUP_ID,
    }

    async def _consumer(self) -> AIOKafkaConsumer:
        consumer = await _consumer_factory(client_id=CLIENT_ID, group_id=GROUP_ID)
        self.addAsyncCleanup(consumer.stop)
        return consumer

    async def test_getone(self):
        consumer = await self._consumer()
        cast(mock.AsyncMock, consumer._fetcher.next_record).side_effect = [_record(TOPIC, 2, 7)]
        await consumer.getone()
        partition_attributes = {
            MESSAGING_DESTINATION_NAME: TOPIC,
            MESSAGING_DESTINATION_PARTITION_ID: "2",
        }
        self.assert_point(
            MESSAGING_CLIENT_OPERATION_DURATION,
            {**self.RECEIVE_ATTRIBUTES, **partition_attributes, MESSAGING_OPERATION_TYPE: "receive"},
            1,
        )
        self.assert_point(
            MESSAGING_CLIENT_CONSUMED_MESSAGES,
            {**self.RECEIVE_ATTRIBUTES, **partition_attributes},
            1,
        )

    async def test_getone_error(self):
        consumer = await self._consumer()
        cast(mock.AsyncMock, consumer._fetcher.next_record).side_effect = KafkaTimeoutError()
        with self.assertRaises(KafkaTimeoutError):
            await consumer.getone()
        self.assert_point(
            MESSAGING_CLIENT_OPERATION_DURATION,
            {
                **self.RECEIVE_ATTRIBUTES,
                MESSAGING_OPERATION_TYPE: "receive",
                ERROR_TYPE: "KafkaTimeoutError",
            },
            1,
        )
        self.assertEqual(self.points(MESSAGING_CLIENT_CONSUMED_MESSAGES), [])

    async def test_getmany(self):
        consumer = await self._consumer()
        cast(mock.AsyncMock, consumer._fetcher.fetched_records).side_effect = [
            {
                TopicPartition(TOPIC, 0): [_record(TOPIC, 0, 1), _record(TOPIC, 0, 2)],
                TopicPartition("topic_2", 1): [_record("topic_2", 1, 5)],
            }
        ]
        await consumer.getmany()
        # several partitions were polled, so the duration has no destination
        self.assert_point(
            MESSAGING_CLIENT_OPERATION_DURATION,
            {**self.RECEIVE_ATTRIBUTES, MESSAGING_OPERATION_TYPE: "receive"},
            1,
        )
        self.assert_point(
            MESSAGING_CLIENT_CONSUMED_MESSAGES,
            {**self.RECEIVE_ATTRIBUTES, MESSAGING_DESTINATION_NAME: TOPIC, MESSAGING_DESTINATION_PARTITION_ID: "0"},
            2,
        )
        self.assert_point(
            MESSAGING_CLIENT_CONSUMED_MESSAGES,
            {
                **self.RECEIVE_ATTRIBUTES,
                MESSAGING_DESTINATION_NAME: "topic_2",
                MESSAGING_DESTINATION_PARTITION_ID: "1",
            },
            1,
        )

    async def test_getmany_empty(self):
        consumer = await self._consumer()
        cast(mock.AsyncMock, consumer._fetcher.fetched_records).side_effect = [{}]
        await consumer.getmany()
        self.assert_point(
            MESSAGING_CLIENT_OPERATION_DURATION,
            {**self.RECEIVE_ATTRIBUTES, MESSAGING_OPERATION_TYPE: "receive"},
            1,
        )
        self.assertEqual(self.points(MESSAGING_CLIENT_CONSUMED_MESSAGES), [])

    async def test_consumer_lag(self):
        consumer = await self._consumer()
        assigned = TopicPartition(TOPIC, 0)
        revoked = TopicPartition("topic_2", 1)
        cast(mock.AsyncMock, consumer._fetcher.fetched_records).side_effect = [
            {assigned: [_record(TOPIC, 0, 4)], revoked: [_record("topic_2", 1, 1)]}
        ]
        await consumer.getmany()
        consumer.assignment = mock.Mock(return_value={assigned})
        consumer.highwater = mock.Mock(return_value=10)
        points = self.points(MESSAGING_KAFKA_CONSUMER_LAG)
        self.assertEqual(len(points), 1, points)
        self.assertEqual(points[0].value, 5)
        self.assertEqual(
            dict(points[0].attributes),
            {
                MESSAGING_SYSTEM: "kafka",
                MESSAGING_DESTINATION_NAME: TOPIC,
                MESSAGING_DESTINATION_PARTITION_ID: "0",
                MESSAGING_CLIENT_ID: CLIENT_ID,
                MESSAGING_CONSUMER_GROUP_NAME: GROUP_ID,
            },
        )


class TestAIOKafkaMetricsDefault(_MetricsTestBase):
    opt_in = ""

    async def test_no_metrics_without_opt_in(self):
        consumer = await _consumer_factory(client_id=CLIENT_ID, group_id=GROUP_ID)
        self.addAsyncCleanup(consumer.stop)
        cast(mock.AsyncMock, consumer._fetcher.next_record).side_effect = [_record(TOPIC, 0, 1)]
        await consumer.getone()
        self.assertIsNone(self.memory_metrics_reader.get_metrics_data())
