# Copyright The OpenTelemetry Authors
# SPDX-License-Identifier: Apache-2.0

"""Messaging metrics, reported with the ``messaging`` semantic convention opt-in."""

from __future__ import annotations

import asyncio
import weakref
from collections.abc import Callable, Iterable
from logging import getLogger
from timeit import default_timer
from typing import TYPE_CHECKING

from opentelemetry.instrumentation._semconv import (
    HTTP_DURATION_HISTOGRAM_BUCKETS_NEW,
)
from opentelemetry.metrics import CallbackOptions, Meter, Observation
from opentelemetry.semconv._incubating.attributes.messaging_attributes import (
    MESSAGING_CLIENT_ID,
    MESSAGING_CONSUMER_GROUP_NAME,
    MESSAGING_DESTINATION_NAME,
    MESSAGING_DESTINATION_PARTITION_ID,
    MESSAGING_OPERATION_NAME,
    MESSAGING_OPERATION_TYPE,
    MESSAGING_SYSTEM,
    MessagingOperationTypeValues,
    MessagingSystemValues,
)
from opentelemetry.semconv._incubating.metrics.messaging_metrics import (
    MESSAGING_CLIENT_OPERATION_DURATION,
    create_messaging_client_consumed_messages,
    create_messaging_client_sent_messages,
)
from opentelemetry.semconv.attributes.error_attributes import ERROR_TYPE

if TYPE_CHECKING:
    import aiokafka
    from aiokafka.structs import RecordMetadata

_LOG = getLogger(__name__)

# There is no semantic convention for the consumer lag yet, the name follows the
# messaging.kafka.* namespace used by the Kafka specific attributes.
MESSAGING_KAFKA_CONSUMER_LAG = "messaging.kafka.consumer.lag"

_SEND_OPERATION = "send"
_RECEIVE_OPERATION = "receive"


class KafkaMetrics:
    def __init__(self, meter: Meter) -> None:
        self._operation_duration = meter.create_histogram(
            name=MESSAGING_CLIENT_OPERATION_DURATION,
            description="Duration of messaging operation initiated by a producer or consumer client.",
            unit="s",
            explicit_bucket_boundaries_advisory=HTTP_DURATION_HISTOGRAM_BUCKETS_NEW,
        )
        self._sent_messages = create_messaging_client_sent_messages(meter)
        self._consumed_messages = create_messaging_client_consumed_messages(meter)
        # last consumed offset of every partition, per consumer
        self._offsets: weakref.WeakKeyDictionary[aiokafka.AIOKafkaConsumer, dict[aiokafka.TopicPartition, int]] = (
            weakref.WeakKeyDictionary()
        )
        meter.create_observable_gauge(
            name=MESSAGING_KAFKA_CONSUMER_LAG,
            callbacks=[self._observe_lag],
            unit="{message}",
            description="The number of messages between the last consumed offset and the end of the partition.",
        )

    @staticmethod
    def _base_attributes(operation_name: str) -> dict[str, str]:
        return {
            MESSAGING_SYSTEM: MessagingSystemValues.KAFKA.value,
            MESSAGING_OPERATION_NAME: operation_name,
        }

    def record_send(
        self,
        start: float,
        topic: str,
        partition: int | None,
        error: BaseException | None,
    ) -> None:
        attributes = self._base_attributes(_SEND_OPERATION)
        attributes[MESSAGING_DESTINATION_NAME] = topic
        if partition is not None:
            attributes[MESSAGING_DESTINATION_PARTITION_ID] = str(partition)
        if error is not None:
            attributes[ERROR_TYPE] = type(error).__qualname__
        self._sent_messages.add(1, attributes)
        self._operation_duration.record(
            max(default_timer() - start, 0),
            {**attributes, MESSAGING_OPERATION_TYPE: MessagingOperationTypeValues.SEND.value},
        )

    def record_send_result(
        self,
        start: float,
        topic: str,
        partition: int | None,
        future: asyncio.Future[RecordMetadata],
    ) -> None:
        """Records the send once the broker acknowledged (or rejected) the message."""

        def _done(done_future: asyncio.Future[RecordMetadata]) -> None:
            if done_future.cancelled():
                self.record_send(start, topic, partition, asyncio.CancelledError())
                return
            error = done_future.exception()
            if error is None:
                partition_id = getattr(done_future.result(), "partition", partition)
                self.record_send(start, topic, partition_id, None)
            else:
                self.record_send(start, topic, partition, error)

        future.add_done_callback(_done)

    def record_receive(
        self,
        start: float,
        consumer: aiokafka.AIOKafkaConsumer,
        consumer_group: str | None,
        records: dict[aiokafka.TopicPartition, list[aiokafka.ConsumerRecord[object, object]]],
        error: BaseException | None,
    ) -> None:
        attributes = self._base_attributes(_RECEIVE_OPERATION)
        if consumer_group is not None:
            attributes[MESSAGING_CONSUMER_GROUP_NAME] = consumer_group
        duration_attributes = {
            **attributes,
            MESSAGING_OPERATION_TYPE: MessagingOperationTypeValues.RECEIVE.value,
        }
        if len(records) == 1:
            topic_partition = next(iter(records))
            duration_attributes[MESSAGING_DESTINATION_NAME] = topic_partition.topic
            duration_attributes[MESSAGING_DESTINATION_PARTITION_ID] = str(topic_partition.partition)
        if error is not None:
            duration_attributes[ERROR_TYPE] = type(error).__qualname__
        self._operation_duration.record(max(default_timer() - start, 0), duration_attributes)

        if not records:
            return
        offsets = self._offsets.setdefault(consumer, {})
        for topic_partition, partition_records in records.items():
            if not partition_records:
                continue
            offsets[topic_partition] = partition_records[-1].offset
            self._consumed_messages.add(
                len(partition_records),
                {
                    **attributes,
                    MESSAGING_DESTINATION_NAME: topic_partition.topic,
                    MESSAGING_DESTINATION_PARTITION_ID: str(topic_partition.partition),
                },
            )

    def _observe_lag(self, options: CallbackOptions) -> Iterable[Observation]:
        for consumer, offsets in list(self._offsets.items()):
            try:
                assignment = consumer.assignment()
            except Exception as exception:  # pylint: disable=broad-except
                _LOG.debug("Unable to get the consumer assignment: %s", exception)
                continue
            consumer_group = getattr(consumer, "_group_id", None)
            client_id = getattr(getattr(consumer, "_client", None), "_client_id", None)
            for topic_partition, offset in list(offsets.items()):
                if topic_partition not in assignment:
                    offsets.pop(topic_partition, None)
                    continue
                # aiokafka annotates highwater() as returning None, it returns the offset or None
                get_highwater: Callable[[aiokafka.TopicPartition], int | None] = getattr(consumer, "highwater")  # noqa: B009
                highwater = get_highwater(topic_partition)
                if highwater is None:
                    continue
                attributes = {
                    MESSAGING_SYSTEM: MessagingSystemValues.KAFKA.value,
                    MESSAGING_DESTINATION_NAME: topic_partition.topic,
                    MESSAGING_DESTINATION_PARTITION_ID: str(topic_partition.partition),
                }
                if client_id is not None:
                    attributes[MESSAGING_CLIENT_ID] = client_id
                if consumer_group is not None:
                    attributes[MESSAGING_CONSUMER_GROUP_NAME] = consumer_group
                yield Observation(max(highwater - offset - 1, 0), attributes)
