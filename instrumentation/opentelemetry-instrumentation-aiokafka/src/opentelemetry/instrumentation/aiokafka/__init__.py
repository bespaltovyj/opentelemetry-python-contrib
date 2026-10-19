# Copyright The OpenTelemetry Authors
# SPDX-License-Identifier: Apache-2.0

"""
Instrument aiokafka to report instrumentation-kafka produced and consumed messages

Usage
-----

.. code:: python

    import asyncio
    from opentelemetry.instrumentation.aiokafka import AIOKafkaInstrumentor
    from aiokafka import AIOKafkaProducer, AIOKafkaConsumer

    # Instrument kafka
    AIOKafkaInstrumentor().instrument()

    # report a span of type producer with the default settings
    async def produce():
        producer = AIOKafkaProducer(bootstrap_servers=['localhost:9092'])
        await producer.start()
        try:
            await producer.send_and_wait('my-topic', b'raw_bytes')
        finally:
            await producer.stop()

    # report a span of type consumer with the default settings
    async def consume():
        consumer = AIOKafkaConsumer('my-topic', group_id='my-group', bootstrap_servers=['localhost:9092'])
        await consumer.start()
        try:
            async for message in consumer:
                # process message
                print(message)
        finally:
            await consumer.stop()

    asyncio.run(produce())
    asyncio.run(consume())

The ``_instrument()`` method accepts the following keyword args:

- **tracer_provider** (TracerProvider) - an optional tracer provider
- **async_produce_hook** (Callable) - a function with extra user-defined logic to be performed before sending the message

  Function signature:

  .. code:: python

      async def async_produce_hook(span: Span, args, kwargs): ...

- **async_consume_hook** (Callable) - a function with extra user-defined logic to be performed after consuming a message

  Function signature:

  .. code:: python

      async def async_consume_hook(span: Span, record: kafka.record.ABCRecord, args, kwargs): ...

For example:

.. code:: python

    import asyncio
    from opentelemetry.instrumentation.aiokafka import AIOKafkaInstrumentor
    from aiokafka import AIOKafkaProducer, AIOKafkaConsumer

    async def async_produce_hook(span, args, kwargs):
        if span and span.is_recording():
            span.set_attribute("custom_user_attribute_from_async_response_hook", "some-value")

    async def async_consume_hook(span, record, args, kwargs):
        if span and span.is_recording():
            span.set_attribute("custom_user_attribute_from_consume_hook", "some-value")

    # instrument kafka with produce and consume hooks
    AIOKafkaInstrumentor().instrument(async_produce_hook=async_produce_hook, async_consume_hook=async_consume_hook)

    # Using kafka as normal now will automatically generate spans,
    # including user custom attributes added from the hooks
    async def produce():
        producer = AIOKafkaProducer(bootstrap_servers=['localhost:9092'])
        await producer.start()
        try:
            await producer.send_and_wait('my-topic', b'raw_bytes')
        finally:
            await producer.stop()

    asyncio.run(produce())

Metrics
*******

When the ``OTEL_SEMCONV_STABILITY_OPT_IN`` environment variable contains
``messaging`` (or ``messaging/dup``), the following metrics are reported with
the ``messaging.system``, ``messaging.operation.name``,
``messaging.destination.name``, ``messaging.destination.partition.id``,
``messaging.consumer.group.name`` (consumers only) and, on failures,
``error.type`` attributes:

- ``messaging.client.operation.duration``: for ``send`` it is measured until the
  broker acknowledged the message, for ``getone``/``getmany`` it is the duration
  of the call
- ``messaging.client.sent.messages``
- ``messaging.client.consumed.messages``
- ``messaging.kafka.consumer.lag``: gauge with the number of messages between
  the last consumed offset and the high watermark of every assigned partition
  (not defined by the semantic conventions yet)

``messaging.process.duration`` is not reported because the records are
processed by the application after ``getone``/``getmany`` returned.

API
---
"""

from __future__ import annotations

from collections.abc import Collection
from inspect import iscoroutinefunction
from typing import TYPE_CHECKING

import aiokafka
from wrapt import (
    wrap_function_wrapper,
)

from opentelemetry import trace
from opentelemetry.instrumentation._semconv import (
    _OpenTelemetrySemanticConventionStability,
    _OpenTelemetryStabilitySignalType,
    _report_new,
)
from opentelemetry.instrumentation.aiokafka._metrics import KafkaMetrics
from opentelemetry.instrumentation.aiokafka.package import _instruments
from opentelemetry.instrumentation.aiokafka.utils import (
    _wrap_getmany,
    _wrap_getone,
    _wrap_send,
)
from opentelemetry.instrumentation.aiokafka.version import __version__
from opentelemetry.instrumentation.instrumentor import BaseInstrumentor
from opentelemetry.instrumentation.utils import unwrap
from opentelemetry.metrics import MeterProvider, get_meter
from opentelemetry.semconv.schemas import Schemas

if TYPE_CHECKING:
    from typing import TypedDict

    from typing_extensions import Unpack

    from .utils import ConsumeHookT, ProduceHookT

    class InstrumentKwargs(TypedDict, total=False):
        tracer_provider: trace.TracerProvider
        meter_provider: MeterProvider
        async_produce_hook: ProduceHookT
        async_consume_hook: ConsumeHookT

    class UninstrumentKwargs(TypedDict, total=False):
        pass


class AIOKafkaInstrumentor(BaseInstrumentor):
    """An instrumentor for kafka module
    See `BaseInstrumentor`
    """

    def instrumentation_dependencies(self) -> Collection[str]:
        return _instruments

    def _instrument(self, **kwargs: Unpack[InstrumentKwargs]):
        """Instruments the kafka module

        Args:
            **kwargs: Optional arguments
                ``tracer_provider``: a TracerProvider, defaults to global.
                ``meter_provider``: a MeterProvider, defaults to global.
                ``async_produce_hook``: a callable to be executed just before producing a message
                ``async_consume_hook``: a callable to be executed just after consuming a message
        """
        tracer_provider = kwargs.get("tracer_provider")

        async_produce_hook = kwargs.get("async_produce_hook")
        if not iscoroutinefunction(async_produce_hook):
            async_produce_hook = None

        async_consume_hook = kwargs.get("async_consume_hook")
        if not iscoroutinefunction(async_consume_hook):
            async_consume_hook = None

        tracer = trace.get_tracer(
            __name__,
            __version__,
            tracer_provider=tracer_provider,
            schema_url=Schemas.V1_27_0.value,
        )

        _OpenTelemetrySemanticConventionStability._initialize()
        metrics = None
        if _report_new(
            _OpenTelemetrySemanticConventionStability._get_opentelemetry_stability_opt_in_mode(
                _OpenTelemetryStabilitySignalType.MESSAGING,
            )
        ):
            meter = get_meter(
                __name__,
                __version__,
                meter_provider=kwargs.get("meter_provider"),
                schema_url=Schemas.V1_27_0.value,
            )
            metrics = KafkaMetrics(meter)

        wrap_function_wrapper(
            aiokafka.AIOKafkaProducer,
            "send",
            _wrap_send(tracer, async_produce_hook, metrics),
        )
        wrap_function_wrapper(
            aiokafka.AIOKafkaConsumer,
            "getone",
            _wrap_getone(tracer, async_consume_hook, metrics),
        )
        wrap_function_wrapper(
            aiokafka.AIOKafkaConsumer,
            "getmany",
            _wrap_getmany(tracer, async_consume_hook, metrics),
        )

    def _uninstrument(self, **kwargs: Unpack[UninstrumentKwargs]):
        unwrap(aiokafka.AIOKafkaProducer, "send")
        unwrap(aiokafka.AIOKafkaConsumer, "getone")
        unwrap(aiokafka.AIOKafkaConsumer, "getmany")
