# Copyright The OpenTelemetry Authors
# SPDX-License-Identifier: Apache-2.0

"""Event loop metrics.

There are no semantic conventions for the Python event loop yet, the delay
metric is modeled after ``nodejs.eventloop.delay.*``.
"""

from __future__ import annotations

import asyncio
import weakref
from collections.abc import Iterable

from opentelemetry.metrics import CallbackOptions, Meter, Observation

ASYNCIO_EVENTLOOP_DELAY = "asyncio.eventloop.delay"
ASYNCIO_TASK_COUNT = "asyncio.task.count"

# how often the event loop delay is sampled, in seconds
_PROBE_INTERVAL = 0.1
# event loop delay buckets, in seconds
_DELAY_BUCKETS = (0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10)


class _EventLoopMetrics:
    def __init__(self, meter: Meter) -> None:
        self._loops: weakref.WeakSet[asyncio.AbstractEventLoop] = weakref.WeakSet()
        # the time each loop was last (re)started at: a probe scheduled before a restart
        # must not count the time the loop was stopped as a delay
        self._started_at: weakref.WeakKeyDictionary[asyncio.AbstractEventLoop, float] = weakref.WeakKeyDictionary()
        self._enabled = True
        self._delay = meter.create_histogram(
            name=ASYNCIO_EVENTLOOP_DELAY,
            unit="s",
            description="Delay of the callbacks scheduled on the event loop, sampled every 100ms.",
            explicit_bucket_boundaries_advisory=_DELAY_BUCKETS,
        )
        meter.create_observable_up_down_counter(
            name=ASYNCIO_TASK_COUNT,
            callbacks=[self._observe_tasks],
            unit="{task}",
            description="The number of tasks that are not done yet.",
        )

    def monitor(self, loop: asyncio.AbstractEventLoop) -> None:
        """Starts sampling the delay of the loop which is about to run (or is running)."""
        if not self._enabled:
            return
        self._started_at[loop] = loop.time()
        if loop in self._loops:
            # the probe scheduled during a previous run is still pending
            return
        self._loops.add(loop)
        # call_soon_threadsafe because the loop may be started from another thread
        loop.call_soon_threadsafe(self._schedule_probe, loop)

    def stop(self) -> None:
        """Stops the sampling, the pending probes are discarded when they run."""
        self._enabled = False
        self._loops.clear()
        self._started_at.clear()

    def _schedule_probe(self, loop: asyncio.AbstractEventLoop) -> None:
        if not self._enabled or loop.is_closed():
            return
        expected = loop.time() + _PROBE_INTERVAL
        loop.call_at(expected, self._probe, loop, expected)

    def _probe(self, loop: asyncio.AbstractEventLoop, expected: float) -> None:
        if not self._enabled:
            return
        self._delay.record(max(loop.time() - max(expected, self._started_at.get(loop, expected)), 0))
        self._schedule_probe(loop)

    def _observe_tasks(self, options: CallbackOptions) -> Iterable[Observation]:
        total = 0
        for loop in list(self._loops):
            if loop.is_closed():
                continue
            try:
                total += sum(1 for task in asyncio.all_tasks(loop) if not task.done())
            except RuntimeError:
                # the tasks of the loop changed while they were iterated
                continue
        if self._loops:
            yield Observation(total)
