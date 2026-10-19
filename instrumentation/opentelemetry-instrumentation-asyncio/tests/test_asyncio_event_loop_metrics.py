# Copyright The OpenTelemetry Authors
# SPDX-License-Identifier: Apache-2.0
import asyncio
import time
from unittest.mock import patch

# pylint: disable=no-name-in-module
from opentelemetry.instrumentation.asyncio import AsyncioInstrumentor
from opentelemetry.instrumentation.asyncio._metrics import (
    ASYNCIO_EVENTLOOP_DELAY,
    ASYNCIO_TASK_COUNT,
)
from opentelemetry.instrumentation.asyncio.environment_variables import (
    OTEL_PYTHON_ASYNCIO_EVENT_LOOP_METRICS_ENABLED,
)
from opentelemetry.sdk.metrics.export import (
    HistogramDataPoint,
    NumberDataPoint,
)
from opentelemetry.test.test_base import TestBase

SCOPE = "opentelemetry.instrumentation.asyncio"


class TestAsyncioEventLoopMetrics(TestBase):
    def setUp(self):
        super().setUp()
        with patch.dict("os.environ", {OTEL_PYTHON_ASYNCIO_EVENT_LOOP_METRICS_ENABLED: "true"}):
            AsyncioInstrumentor().instrument(meter_provider=self.meter_provider)

    def tearDown(self):
        AsyncioInstrumentor().uninstrument()
        super().tearDown()

    def metric(self, name):
        for metric in self.get_sorted_metrics(SCOPE):
            if metric.name == name:
                return metric
        return None

    def test_event_loop_delay(self):
        async def main():
            await asyncio.sleep(0.15)
            # block the loop while the next probe is due
            time.sleep(0.3)
            await asyncio.sleep(0.05)

        asyncio.run(main())
        metric = self.metric(ASYNCIO_EVENTLOOP_DELAY)
        self.assertEqual(metric.unit, "s")
        (point,) = metric.data.data_points
        self.assertIsInstance(point, HistogramDataPoint)
        self.assertEqual(dict(point.attributes), {})
        self.assertGreaterEqual(point.count, 2)
        self.assertGreaterEqual(point.max, 0.15)

    def test_stopped_loop_is_not_a_delay(self):
        loop = asyncio.new_event_loop()
        try:
            loop.run_until_complete(asyncio.sleep(0.01))
            # the probe scheduled during the first run is due while the loop is stopped
            time.sleep(0.3)
            loop.run_until_complete(asyncio.sleep(0.15))
        finally:
            loop.close()
        (point,) = self.metric(ASYNCIO_EVENTLOOP_DELAY).data.data_points
        self.assertLess(point.max, 0.15)

    def test_task_count(self):
        counts = []

        async def main():
            tasks = [asyncio.create_task(asyncio.sleep(0.2)) for _ in range(3)]
            await asyncio.sleep(0)
            counts.append(self.metric(ASYNCIO_TASK_COUNT))
            await asyncio.gather(*tasks)

        asyncio.run(main())
        (point,) = counts[0].data.data_points
        self.assertIsInstance(point, NumberDataPoint)
        # the main task and the three sleeping tasks
        self.assertEqual(point.value, 4)
        self.assertEqual(dict(point.attributes), {})

    def test_running_loop_is_monitored(self):
        AsyncioInstrumentor().uninstrument()

        async def main():
            with patch.dict("os.environ", {OTEL_PYTHON_ASYNCIO_EVENT_LOOP_METRICS_ENABLED: "true"}):
                AsyncioInstrumentor().instrument(meter_provider=self.meter_provider)
            await asyncio.sleep(0.25)

        asyncio.run(main())
        self.assertIsNotNone(self.metric(ASYNCIO_EVENTLOOP_DELAY))


class TestAsyncioEventLoopMetricsDisabled(TestBase):
    def test_no_event_loop_metrics_by_default(self):
        AsyncioInstrumentor().instrument(meter_provider=self.meter_provider)
        try:
            asyncio.run(asyncio.sleep(0.15))
        finally:
            AsyncioInstrumentor().uninstrument()
        names = {metric.name for metric in self.get_sorted_metrics(SCOPE)}
        self.assertNotIn(ASYNCIO_EVENTLOOP_DELAY, names)
        self.assertNotIn(ASYNCIO_TASK_COUNT, names)
