# Copyright The OpenTelemetry Authors
# SPDX-License-Identifier: Apache-2.0
"""
Instrument threading to propagate OpenTelemetry context.

Usage
-----

.. code-block:: python

    from opentelemetry.instrumentation.threading import ThreadingInstrumentor

    ThreadingInstrumentor().instrument()

This library provides instrumentation for the `threading` module to ensure that
the OpenTelemetry context is propagated across threads. By default this
instrumentation does not produce any telemetry data on its own. It merely
ensures that the context is correctly propagated when threads are used.


When instrumented, new threads created using threading.Thread, threading.Timer,
or within futures.ThreadPoolExecutor will have the current OpenTelemetry
context attached, and this context will be re-activated in the thread's
run method or the executor's worker thread."

Executor metrics
----------------

Set the ``OTEL_PYTHON_THREADING_EXECUTOR_METRICS_ENABLED`` environment variable
to ``true`` to report the following metrics for ``futures.ThreadPoolExecutor``,
all with the ``executor.name`` attribute (the ``thread_name_prefix`` of the
executor, or ``ThreadPoolExecutor`` when it was not set):

- ``executor.pool.size`` and ``executor.pool.max``: current and maximum number of
  worker threads
- ``executor.queued``: number of tasks waiting in the queue
- ``executor.active``: number of tasks being executed
- ``executor.completed``: number of executed tasks, with ``error.type`` when the
  task raised an exception
- ``executor.task.duration`` and ``executor.task.queue_time``: execution time
  and time spent in the queue

These metrics are not defined by the semantic conventions yet; their names
follow the executor metrics of Micrometer. A custom meter provider can be passed
with ``instrument(meter_provider=...)``.
"""

from __future__ import annotations

import functools
import os
import threading
from collections.abc import Callable, Collection
from concurrent import futures
from typing import TYPE_CHECKING, Any

from wrapt import (
    wrap_function_wrapper,
)

from opentelemetry import context
from opentelemetry.instrumentation.instrumentor import BaseInstrumentor
from opentelemetry.instrumentation.threading._metrics import ExecutorMetrics
from opentelemetry.instrumentation.threading.package import _instruments
from opentelemetry.instrumentation.threading.version import __version__
from opentelemetry.instrumentation.utils import unwrap
from opentelemetry.metrics import get_meter

OTEL_PYTHON_THREADING_EXECUTOR_METRICS_ENABLED = "OTEL_PYTHON_THREADING_EXECUTOR_METRICS_ENABLED"

if TYPE_CHECKING:
    from typing import Protocol, TypeVar

    R = TypeVar("R")

    class HasOtelContext(Protocol):
        _otel_context: context.Context


class ThreadingInstrumentor(BaseInstrumentor):
    __WRAPPER_START_METHOD = "start"
    __WRAPPER_RUN_METHOD = "run"
    __WRAPPER_SUBMIT_METHOD = "submit"
    _executor_metrics: ExecutorMetrics | None = None

    def instrumentation_dependencies(self) -> Collection[str]:
        return _instruments

    def _instrument(self, **kwargs: Any):
        if os.environ.get(OTEL_PYTHON_THREADING_EXECUTOR_METRICS_ENABLED, "false").strip().lower() == "true":
            meter = get_meter(__name__, __version__, kwargs.get("meter_provider"))
            ThreadingInstrumentor._executor_metrics = ExecutorMetrics(meter)
        self._instrument_thread()
        self._instrument_timer()
        self._instrument_thread_pool()

    def _uninstrument(self, **kwargs: Any):
        self._uninstrument_thread()
        self._uninstrument_timer()
        self._uninstrument_thread_pool()
        ThreadingInstrumentor._executor_metrics = None

    @staticmethod
    def _instrument_thread():
        wrap_function_wrapper(
            threading.Thread,
            ThreadingInstrumentor.__WRAPPER_START_METHOD,
            ThreadingInstrumentor.__wrap_threading_start,
        )
        wrap_function_wrapper(
            threading.Thread,
            ThreadingInstrumentor.__WRAPPER_RUN_METHOD,
            ThreadingInstrumentor.__wrap_threading_run,
        )

    @staticmethod
    def _instrument_timer():
        wrap_function_wrapper(
            threading.Timer,
            ThreadingInstrumentor.__WRAPPER_START_METHOD,
            ThreadingInstrumentor.__wrap_threading_start,
        )
        wrap_function_wrapper(
            threading.Timer,
            ThreadingInstrumentor.__WRAPPER_RUN_METHOD,
            ThreadingInstrumentor.__wrap_threading_run,
        )

    @staticmethod
    def _instrument_thread_pool():
        wrap_function_wrapper(
            futures.ThreadPoolExecutor,
            ThreadingInstrumentor.__WRAPPER_SUBMIT_METHOD,
            ThreadingInstrumentor.__wrap_thread_pool_submit,
        )

    @staticmethod
    def _uninstrument_thread():
        unwrap(threading.Thread, ThreadingInstrumentor.__WRAPPER_START_METHOD)
        unwrap(threading.Thread, ThreadingInstrumentor.__WRAPPER_RUN_METHOD)

    @staticmethod
    def _uninstrument_timer():
        unwrap(threading.Timer, ThreadingInstrumentor.__WRAPPER_START_METHOD)
        unwrap(threading.Timer, ThreadingInstrumentor.__WRAPPER_RUN_METHOD)

    @staticmethod
    def _uninstrument_thread_pool():
        unwrap(
            futures.ThreadPoolExecutor,
            ThreadingInstrumentor.__WRAPPER_SUBMIT_METHOD,
        )

    @staticmethod
    def __wrap_threading_start(
        call_wrapped: Callable[[], None],
        instance: HasOtelContext,
        args: tuple[()],
        kwargs: dict[str, Any],
    ) -> None:
        instance._otel_context = context.get_current()
        return call_wrapped(*args, **kwargs)

    @staticmethod
    def __wrap_threading_run(
        call_wrapped: Callable[..., R],
        instance: HasOtelContext,
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
    ) -> R:
        token = None
        try:
            if hasattr(instance, "_otel_context"):
                token = context.attach(instance._otel_context)
            return call_wrapped(*args, **kwargs)
        finally:
            if token is not None:
                context.detach(token)

    @staticmethod
    def __wrap_thread_pool_submit(
        call_wrapped: Callable[..., R],
        instance: futures.ThreadPoolExecutor,
        args: tuple[Callable[..., Any], ...],
        kwargs: dict[str, Any],
    ) -> R:
        # obtain the original function and wrapped kwargs
        original_func = args[0]
        otel_context = context.get_current()

        @functools.wraps(original_func)
        def wrapped_func(*func_args: Any, **func_kwargs: Any) -> R:
            token = None
            try:
                token = context.attach(otel_context)
                return original_func(*func_args, **func_kwargs)
            finally:
                if token is not None:
                    context.detach(token)

        task: Callable[..., Any] = wrapped_func
        executor_metrics = ThreadingInstrumentor._executor_metrics
        if executor_metrics is not None:
            task = executor_metrics.wrap_task(instance, wrapped_func)

        # replace the original function with the wrapped function
        new_args: tuple[Callable[..., Any], ...] = (task,) + args[1:]
        return call_wrapped(*new_args, **kwargs)
