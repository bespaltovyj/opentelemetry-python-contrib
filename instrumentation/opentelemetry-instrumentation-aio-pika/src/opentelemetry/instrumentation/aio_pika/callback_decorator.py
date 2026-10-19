# Copyright The OpenTelemetry Authors
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from collections.abc import Callable
from timeit import default_timer
from typing import Any

from aio_pika import Queue
from aio_pika.abc import AbstractIncomingMessage

from opentelemetry import context, propagate, trace
from opentelemetry.instrumentation.aio_pika._metrics import _MessagingMetrics
from opentelemetry.instrumentation.aio_pika.span_builder import (
    SpanBuilder,
    _get_server_address,
)
from opentelemetry.instrumentation.utils import is_instrumentation_enabled
from opentelemetry.semconv.trace import MessagingOperationValues
from opentelemetry.trace import Span, Tracer


class CallbackDecorator:
    def __init__(
        self,
        tracer: Tracer,
        queue: Queue,
        metrics: _MessagingMetrics | None = None,
    ):
        self._tracer = tracer
        self._queue = queue
        self._metrics = metrics

    def _get_span(self, message: AbstractIncomingMessage) -> Span | None:
        builder = SpanBuilder(self._tracer)
        builder.set_as_consumer()
        builder.set_operation(MessagingOperationValues.RECEIVE)
        builder.set_destination(message.exchange or message.routing_key)
        builder.set_channel(self._queue.channel)
        builder.set_message(message)
        return builder.build()

    def decorate(self, callback: Callable[[AbstractIncomingMessage], Any]) -> Callable[[AbstractIncomingMessage], Any]:
        async def decorated(message: AbstractIncomingMessage):
            if not is_instrumentation_enabled():
                return await callback(message)
            headers = message.headers or {}
            ctx = propagate.extract(headers)
            token = context.attach(ctx)
            span = self._get_span(message)
            if not span:
                return await callback(message)
            start = default_timer()
            error = None
            try:
                with trace.use_span(span, end_on_exit=True):
                    return_value = await callback(message)
            except BaseException as exc:
                error = exc
                raise
            finally:
                self._record_metrics(start, error)
                if token:
                    context.detach(token)
            return return_value

        return decorated

    def _record_metrics(self, start: float, error: BaseException | None) -> None:
        if self._metrics is None:
            return
        host, port = _get_server_address(self._queue.channel)
        self._metrics.record_process(start, self._queue.name, host, port, error)
