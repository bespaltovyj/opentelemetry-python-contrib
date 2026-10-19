# Copyright The OpenTelemetry Authors
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from collections.abc import Callable
from timeit import default_timer

import aiormq
from aio_pika import Exchange
from aio_pika.abc import AbstractMessage

from opentelemetry import propagate, trace
from opentelemetry.instrumentation.aio_pika._metrics import _MessagingMetrics
from opentelemetry.instrumentation.aio_pika.span_builder import (
    SpanBuilder,
    _get_server_address,
)
from opentelemetry.trace import Span, Tracer

# name of the default exchange, used when the exchange name is empty
_DEFAULT_EXCHANGE = "amq.default"


class PublishDecorator:
    def __init__(
        self,
        tracer: Tracer,
        exchange: Exchange,
        metrics: _MessagingMetrics | None = None,
    ):
        self._tracer = tracer
        self._exchange = exchange
        self._metrics = metrics

    def _get_publish_span(self, message: AbstractMessage, routing_key: str) -> Span | None:
        builder = SpanBuilder(self._tracer)
        builder.set_as_producer()
        builder.set_destination(f"{self._exchange.name},{routing_key}")
        builder.set_channel(self._exchange.channel)
        builder.set_message(message)
        return builder.build()

    def decorate(self, publish: Callable) -> Callable:
        async def decorated_publish(
            message: AbstractMessage, routing_key: str, **kwargs
        ) -> aiormq.abc.ConfirmationFrameType | None:
            span = self._get_publish_span(message, routing_key)
            if not span:
                return await publish(message, routing_key, **kwargs)
            start = default_timer()
            error = None
            try:
                with trace.use_span(span, end_on_exit=True):
                    propagate.inject(message.properties.headers)
                    return_value = await publish(message, routing_key, **kwargs)
            except BaseException as exc:
                error = exc
                raise
            finally:
                self._record_metrics(start, error)
            return return_value

        return decorated_publish

    def _record_metrics(self, start: float, error: BaseException | None) -> None:
        if self._metrics is None:
            return
        host, port = _get_server_address(self._exchange.channel)
        self._metrics.record_publish(start, self._exchange.name or _DEFAULT_EXCHANGE, host, port, error)
