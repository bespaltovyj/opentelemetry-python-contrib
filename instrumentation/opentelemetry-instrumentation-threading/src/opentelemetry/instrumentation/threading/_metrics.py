# Copyright The OpenTelemetry Authors
# SPDX-License-Identifier: Apache-2.0

"""Metrics of ``concurrent.futures.ThreadPoolExecutor`` instances.

There are no semantic conventions for executors yet, the metric names follow
the executor metrics of Micrometer (``ExecutorServiceMetrics``).
"""

from __future__ import annotations

import re
import weakref
from collections import defaultdict
from collections.abc import Callable, Iterable
from concurrent import futures
from timeit import default_timer
from typing import Any, TypeVar

from opentelemetry.metrics import CallbackOptions, Meter, Observation
from opentelemetry.semconv.attributes.error_attributes import ERROR_TYPE

R = TypeVar("R")

EXECUTOR_NAME = "executor.name"
EXECUTOR_POOL_SIZE = "executor.pool.size"
EXECUTOR_POOL_MAX = "executor.pool.max"
EXECUTOR_QUEUED = "executor.queued"
EXECUTOR_ACTIVE = "executor.active"
EXECUTOR_COMPLETED = "executor.completed"
EXECUTOR_TASK_DURATION = "executor.task.duration"
EXECUTOR_TASK_QUEUE_TIME = "executor.task.queue_time"

# executors without an explicit thread name prefix get a unique generated one
_GENERATED_PREFIX = re.compile(r"^ThreadPoolExecutor-\d+$")
_DEFAULT_EXECUTOR_NAME = "ThreadPoolExecutor"


def _executor_name(executor: futures.ThreadPoolExecutor) -> str:
    prefix = getattr(executor, "_thread_name_prefix", None)
    if not prefix or _GENERATED_PREFIX.match(prefix):
        return _DEFAULT_EXECUTOR_NAME
    return prefix


class ExecutorMetrics:
    def __init__(self, meter: Meter) -> None:
        self._executors: weakref.WeakSet[futures.ThreadPoolExecutor] = weakref.WeakSet()
        self._active = meter.create_up_down_counter(
            name=EXECUTOR_ACTIVE,
            unit="{task}",
            description="The number of tasks that are currently executed.",
        )
        self._completed = meter.create_counter(
            name=EXECUTOR_COMPLETED,
            unit="{task}",
            description="The number of tasks that have completed execution.",
        )
        self._task_duration = meter.create_histogram(
            name=EXECUTOR_TASK_DURATION,
            unit="s",
            description="The time it took to execute a task.",
        )
        self._queue_time = meter.create_histogram(
            name=EXECUTOR_TASK_QUEUE_TIME,
            unit="s",
            description="The time a task spent in the queue before its execution started.",
        )
        meter.create_observable_up_down_counter(
            name=EXECUTOR_POOL_SIZE,
            callbacks=[self._observe_pool_size],
            unit="{thread}",
            description="The current number of threads in the pool.",
        )
        meter.create_observable_up_down_counter(
            name=EXECUTOR_POOL_MAX,
            callbacks=[self._observe_pool_max],
            unit="{thread}",
            description="The maximum number of threads in the pool.",
        )
        meter.create_observable_up_down_counter(
            name=EXECUTOR_QUEUED,
            callbacks=[self._observe_queued],
            unit="{task}",
            description="The number of tasks waiting in the queue.",
        )

    def wrap_task(self, executor: futures.ThreadPoolExecutor, func: Callable[..., R]) -> Callable[..., R]:
        """Returns a function which runs ``func`` recording the task metrics."""
        self._executors.add(executor)
        attributes = {EXECUTOR_NAME: _executor_name(executor)}
        submitted = default_timer()

        def measured(*args: Any, **kwargs: Any) -> R:
            start = default_timer()
            self._queue_time.record(max(start - submitted, 0), attributes)
            self._active.add(1, attributes)
            error = None
            try:
                return func(*args, **kwargs)
            except BaseException as exc:
                error = exc
                raise
            finally:
                self._active.add(-1, attributes)
                completed_attributes = attributes
                if error is not None:
                    completed_attributes = {**attributes, ERROR_TYPE: type(error).__qualname__}
                self._task_duration.record(max(default_timer() - start, 0), completed_attributes)
                self._completed.add(1, completed_attributes)

        return measured

    def _observe(self, value: Callable[[futures.ThreadPoolExecutor], int | None]) -> Iterable[Observation]:
        totals: defaultdict[str, int] = defaultdict(int)
        for executor in list(self._executors):
            executor_value = value(executor)
            if executor_value is not None:
                totals[_executor_name(executor)] += executor_value
        for name, total in totals.items():
            yield Observation(total, {EXECUTOR_NAME: name})

    def _observe_pool_size(self, options: CallbackOptions) -> Iterable[Observation]:
        return self._observe(lambda executor: len(getattr(executor, "_threads", ())))

    def _observe_pool_max(self, options: CallbackOptions) -> Iterable[Observation]:
        return self._observe(lambda executor: getattr(executor, "_max_workers", None))

    def _observe_queued(self, options: CallbackOptions) -> Iterable[Observation]:
        def queued(executor: futures.ThreadPoolExecutor) -> int | None:
            work_queue = getattr(executor, "_work_queue", None)
            return work_queue.qsize() if work_queue is not None else None

        return self._observe(queued)
