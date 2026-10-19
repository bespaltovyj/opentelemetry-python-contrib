# Copyright The OpenTelemetry Authors
# SPDX-License-Identifier: Apache-2.0

"""Messaging metrics, reported with the ``messaging`` semantic convention opt-in."""

from __future__ import annotations

from timeit import default_timer

from opentelemetry.instrumentation._semconv import (
    HTTP_DURATION_HISTOGRAM_BUCKETS_NEW,
)
from opentelemetry.metrics import Meter
from opentelemetry.semconv._incubating.attributes.messaging_attributes import (
    MESSAGING_DESTINATION_NAME,
    MESSAGING_OPERATION_NAME,
    MESSAGING_OPERATION_TYPE,
    MESSAGING_SYSTEM,
    MessagingOperationTypeValues,
    MessagingSystemValues,
)
from opentelemetry.semconv._incubating.metrics.messaging_metrics import (
    MESSAGING_CLIENT_OPERATION_DURATION,
    MESSAGING_PROCESS_DURATION,
    create_messaging_client_consumed_messages,
    create_messaging_client_sent_messages,
)
from opentelemetry.semconv.attributes.error_attributes import ERROR_TYPE
from opentelemetry.semconv.attributes.server_attributes import (
    SERVER_ADDRESS,
    SERVER_PORT,
)

_PUBLISH_OPERATION = "publish"
_PROCESS_OPERATION = "process"


class _MessagingMetrics:
    def __init__(self, meter: Meter) -> None:
        self._operation_duration = meter.create_histogram(
            name=MESSAGING_CLIENT_OPERATION_DURATION,
            description="Duration of messaging operation initiated by a producer or consumer client.",
            unit="s",
            explicit_bucket_boundaries_advisory=HTTP_DURATION_HISTOGRAM_BUCKETS_NEW,
        )
        self._process_duration = meter.create_histogram(
            name=MESSAGING_PROCESS_DURATION,
            description="Duration of processing operation.",
            unit="s",
            explicit_bucket_boundaries_advisory=HTTP_DURATION_HISTOGRAM_BUCKETS_NEW,
        )
        self._sent_messages = create_messaging_client_sent_messages(meter)
        self._consumed_messages = create_messaging_client_consumed_messages(meter)

    @staticmethod
    def _attributes(
        operation_name: str,
        destination: str,
        server_address: str | None,
        server_port: int | None,
        error: BaseException | None,
    ) -> dict[str, str | int]:
        attributes: dict[str, str | int] = {
            MESSAGING_SYSTEM: MessagingSystemValues.RABBITMQ.value,
            MESSAGING_OPERATION_NAME: operation_name,
            MESSAGING_DESTINATION_NAME: destination,
        }
        if server_address:
            attributes[SERVER_ADDRESS] = server_address
        if server_port:
            attributes[SERVER_PORT] = server_port
        if error is not None:
            attributes[ERROR_TYPE] = type(error).__qualname__
        return attributes

    def record_publish(
        self,
        start: float,
        exchange: str,
        server_address: str | None,
        server_port: int | None,
        error: BaseException | None,
    ) -> None:
        attributes = self._attributes(_PUBLISH_OPERATION, exchange, server_address, server_port, error)
        self._sent_messages.add(1, attributes)
        self._operation_duration.record(
            max(default_timer() - start, 0),
            {**attributes, MESSAGING_OPERATION_TYPE: MessagingOperationTypeValues.SEND.value},
        )

    def record_process(
        self,
        start: float,
        queue: str,
        server_address: str | None,
        server_port: int | None,
        error: BaseException | None,
    ) -> None:
        attributes = self._attributes(_PROCESS_OPERATION, queue, server_address, server_port, error)
        self._consumed_messages.add(1, attributes)
        self._process_duration.record(max(default_timer() - start, 0), attributes)
