# Copyright The OpenTelemetry Authors
# SPDX-License-Identifier: Apache-2.0
import asyncio
from unittest import mock

from aio_pika import Exchange, Queue

from opentelemetry.instrumentation._semconv import (
    OTEL_SEMCONV_STABILITY_OPT_IN,
    _OpenTelemetrySemanticConventionStability,
)
from opentelemetry.instrumentation.aio_pika import AioPikaInstrumentor
from opentelemetry.instrumentation.aio_pika._metrics import _MessagingMetrics
from opentelemetry.instrumentation.aio_pika.callback_decorator import (
    CallbackDecorator,
)
from opentelemetry.instrumentation.aio_pika.publish_decorator import (
    PublishDecorator,
)
from opentelemetry.sdk.metrics.export import (
    HistogramDataPoint,
    NumberDataPoint,
)
from opentelemetry.semconv._incubating.attributes.messaging_attributes import (
    MESSAGING_DESTINATION_NAME,
    MESSAGING_OPERATION_NAME,
    MESSAGING_OPERATION_TYPE,
    MESSAGING_SYSTEM,
)
from opentelemetry.semconv._incubating.metrics.messaging_metrics import (
    MESSAGING_CLIENT_CONSUMED_MESSAGES,
    MESSAGING_CLIENT_OPERATION_DURATION,
    MESSAGING_CLIENT_SENT_MESSAGES,
    MESSAGING_PROCESS_DURATION,
)
from opentelemetry.semconv.attributes.error_attributes import ERROR_TYPE
from opentelemetry.semconv.attributes.server_attributes import (
    SERVER_ADDRESS,
    SERVER_PORT,
)
from opentelemetry.test.test_base import TestBase

from .consts import (
    AIOPIKA_VERSION_INFO,
    CHANNEL_7,
    CHANNEL_8,
    CONNECTION_7,
    EXCHANGE_NAME,
    MESSAGE,
    QUEUE_NAME,
    ROUTING_KEY,
    SERVER_HOST,
)
from .consts import SERVER_PORT as SERVER_PORT_VALUE

PUBLISH_ATTRIBUTES = {
    MESSAGING_SYSTEM: "rabbitmq",
    MESSAGING_OPERATION_NAME: "publish",
    MESSAGING_DESTINATION_NAME: EXCHANGE_NAME,
    SERVER_ADDRESS: SERVER_HOST,
    SERVER_PORT: SERVER_PORT_VALUE,
}
PROCESS_ATTRIBUTES = {
    MESSAGING_SYSTEM: "rabbitmq",
    MESSAGING_OPERATION_NAME: "process",
    MESSAGING_DESTINATION_NAME: QUEUE_NAME,
    SERVER_ADDRESS: SERVER_HOST,
    SERVER_PORT: SERVER_PORT_VALUE,
}


def _make_exchange(name=EXCHANGE_NAME):
    if AIOPIKA_VERSION_INFO >= (8, 0):
        return Exchange(CHANNEL_8, name)
    return Exchange(CONNECTION_7, CHANNEL_7, name)


def _make_queue():
    channel = CHANNEL_8 if AIOPIKA_VERSION_INFO >= (8, 0) else CHANNEL_7
    return Queue(channel, QUEUE_NAME, False, False, False, None)


class TestAioPikaMetrics(TestBase):
    def setUp(self):
        super().setUp()
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)
        self.metrics = _MessagingMetrics(self.meter_provider.get_meter(__name__))

    def tearDown(self):
        self.loop.close()
        super().tearDown()

    def _points(self, name):
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

    def _assert_point(self, name, attributes, point_type, value):
        points = self._points(name)
        self.assertEqual(len(points), 1, points)
        point = points[0]
        self.assertIsInstance(point, point_type)
        self.assertEqual(dict(point.attributes), attributes)
        self.assertIsInstance(point.attributes[SERVER_PORT], int)
        if point_type is HistogramDataPoint:
            self.assertEqual(point.count, value)
        else:
            self.assertEqual(point.value, value)

    def _publish(self, exchange, publish):
        decorated = PublishDecorator(self.tracer_provider.get_tracer(__name__), exchange, self.metrics).decorate(
            publish
        )
        return self.loop.run_until_complete(decorated(MESSAGE, ROUTING_KEY))

    def _consume(self, callback):
        decorated = CallbackDecorator(self.tracer_provider.get_tracer(__name__), _make_queue(), self.metrics).decorate(
            callback
        )
        return self.loop.run_until_complete(decorated(MESSAGE))

    def test_publish(self):
        publish = mock.AsyncMock(return_value="confirmation")
        self.assertEqual(self._publish(_make_exchange(), publish), "confirmation")
        self._assert_point(MESSAGING_CLIENT_SENT_MESSAGES, PUBLISH_ATTRIBUTES, NumberDataPoint, 1)
        self._assert_point(
            MESSAGING_CLIENT_OPERATION_DURATION,
            {**PUBLISH_ATTRIBUTES, MESSAGING_OPERATION_TYPE: "send"},
            HistogramDataPoint,
            1,
        )

    def test_publish_error(self):
        publish = mock.AsyncMock(side_effect=RuntimeError("closed"))
        with self.assertRaises(RuntimeError):
            self._publish(_make_exchange(), publish)
        attributes = {**PUBLISH_ATTRIBUTES, ERROR_TYPE: "RuntimeError"}
        self._assert_point(MESSAGING_CLIENT_SENT_MESSAGES, attributes, NumberDataPoint, 1)
        self._assert_point(
            MESSAGING_CLIENT_OPERATION_DURATION,
            {**attributes, MESSAGING_OPERATION_TYPE: "send"},
            HistogramDataPoint,
            1,
        )

    def test_publish_default_exchange(self):
        self._publish(_make_exchange(name=""), mock.AsyncMock())
        self._assert_point(
            MESSAGING_CLIENT_SENT_MESSAGES,
            {**PUBLISH_ATTRIBUTES, MESSAGING_DESTINATION_NAME: "amq.default"},
            NumberDataPoint,
            1,
        )

    def test_consume(self):
        callback = mock.AsyncMock(return_value="processed")
        self.assertEqual(self._consume(callback), "processed")
        self._assert_point(MESSAGING_CLIENT_CONSUMED_MESSAGES, PROCESS_ATTRIBUTES, NumberDataPoint, 1)
        self._assert_point(MESSAGING_PROCESS_DURATION, PROCESS_ATTRIBUTES, HistogramDataPoint, 1)

    def test_consume_error(self):
        callback = mock.AsyncMock(side_effect=ValueError("bad message"))
        with self.assertRaises(ValueError):
            self._consume(callback)
        attributes = {**PROCESS_ATTRIBUTES, ERROR_TYPE: "ValueError"}
        self._assert_point(MESSAGING_CLIENT_CONSUMED_MESSAGES, attributes, NumberDataPoint, 1)
        self._assert_point(MESSAGING_PROCESS_DURATION, attributes, HistogramDataPoint, 1)


class TestAioPikaInstrumentorMetrics(TestBase):
    def tearDown(self):
        AioPikaInstrumentor().uninstrument()
        _OpenTelemetrySemanticConventionStability._initialized = False
        super().tearDown()

    def _metrics_passed_to_publish_decorator(self):
        _OpenTelemetrySemanticConventionStability._initialized = False
        AioPikaInstrumentor().instrument(meter_provider=self.meter_provider)
        with mock.patch(
            "opentelemetry.instrumentation.aio_pika.aio_pika_instrumentor.PublishDecorator"
        ) as publish_decorator:
            publish_decorator.return_value.decorate.return_value = mock.AsyncMock()
            loop = asyncio.new_event_loop()
            try:
                loop.run_until_complete(Exchange.publish(_make_exchange(), MESSAGE, ROUTING_KEY))
            finally:
                loop.close()
        return publish_decorator.call_args.args[2]

    @mock.patch.dict("os.environ", {OTEL_SEMCONV_STABILITY_OPT_IN: "messaging"})
    def test_metrics_enabled_with_messaging_opt_in(self):
        self.assertIsInstance(self._metrics_passed_to_publish_decorator(), _MessagingMetrics)

    @mock.patch.dict("os.environ", {OTEL_SEMCONV_STABILITY_OPT_IN: ""})
    def test_metrics_disabled_by_default(self):
        self.assertIsNone(self._metrics_passed_to_publish_decorator())
