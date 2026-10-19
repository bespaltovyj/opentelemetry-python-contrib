# Copyright The OpenTelemetry Authors
# SPDX-License-Identifier: Apache-2.0

import threading
from concurrent import futures
from unittest import mock

from opentelemetry.instrumentation.threading import (
    OTEL_PYTHON_THREADING_EXECUTOR_METRICS_ENABLED,
    ThreadingInstrumentor,
)
from opentelemetry.instrumentation.threading._metrics import (
    EXECUTOR_ACTIVE,
    EXECUTOR_COMPLETED,
    EXECUTOR_NAME,
    EXECUTOR_POOL_MAX,
    EXECUTOR_POOL_SIZE,
    EXECUTOR_QUEUED,
    EXECUTOR_TASK_DURATION,
    EXECUTOR_TASK_QUEUE_TIME,
)
from opentelemetry.sdk.metrics.export import (
    HistogramDataPoint,
    NumberDataPoint,
)
from opentelemetry.semconv.attributes.error_attributes import ERROR_TYPE
from opentelemetry.test.test_base import TestBase

SCOPE = "opentelemetry.instrumentation.threading"


class TestExecutorMetrics(TestBase):
    def setUp(self):
        super().setUp()
        with mock.patch.dict("os.environ", {OTEL_PYTHON_THREADING_EXECUTOR_METRICS_ENABLED: "true"}):
            ThreadingInstrumentor().instrument(meter_provider=self.meter_provider)

    def tearDown(self):
        ThreadingInstrumentor().uninstrument()
        super().tearDown()

    def metrics(self):
        return {metric.name: metric for metric in self.get_sorted_metrics(SCOPE)}

    def point(self, name, attributes):
        points = [point for point in self.metrics()[name].data.data_points if dict(point.attributes) == attributes]
        self.assertEqual(len(points), 1, self.metrics()[name].data.data_points)
        return points[0]

    def test_task_metrics(self):
        with futures.ThreadPoolExecutor(max_workers=2, thread_name_prefix="workers") as executor:
            executor.submit(lambda: 1).result()
            executor.submit(lambda: 2).result()
        attributes = {EXECUTOR_NAME: "workers"}
        completed = self.point(EXECUTOR_COMPLETED, attributes)
        self.assertIsInstance(completed, NumberDataPoint)
        self.assertEqual(completed.value, 2)
        self.assertEqual(self.point(EXECUTOR_ACTIVE, attributes).value, 0)
        for name in (EXECUTOR_TASK_DURATION, EXECUTOR_TASK_QUEUE_TIME):
            point = self.point(name, attributes)
            self.assertIsInstance(point, HistogramDataPoint)
            self.assertEqual(point.count, 2)
            self.assertEqual(self.metrics()[name].unit, "s")

    def test_task_error(self):
        def fail():
            raise ValueError("boom")

        with futures.ThreadPoolExecutor(max_workers=1, thread_name_prefix="workers") as executor:
            with self.assertRaises(ValueError):
                executor.submit(fail).result()
        attributes = {EXECUTOR_NAME: "workers", ERROR_TYPE: "ValueError"}
        self.assertEqual(self.point(EXECUTOR_COMPLETED, attributes).value, 1)
        self.assertEqual(self.point(EXECUTOR_TASK_DURATION, attributes).count, 1)

    def test_pool_metrics(self):
        started = threading.Event()
        release = threading.Event()

        def block():
            started.set()
            release.wait(5)

        with futures.ThreadPoolExecutor(max_workers=1, thread_name_prefix="workers") as executor:
            running = executor.submit(block)
            started.wait(5)
            waiting = executor.submit(lambda: None)
            attributes = {EXECUTOR_NAME: "workers"}
            self.assertEqual(self.point(EXECUTOR_POOL_SIZE, attributes).value, 1)
            self.assertEqual(self.point(EXECUTOR_POOL_MAX, attributes).value, 1)
            self.assertEqual(self.point(EXECUTOR_QUEUED, attributes).value, 1)
            self.assertEqual(self.point(EXECUTOR_ACTIVE, attributes).value, 1)
            release.set()
            running.result()
            waiting.result()

    def test_default_executor_name(self):
        with futures.ThreadPoolExecutor(max_workers=1) as executor:
            executor.submit(lambda: None).result()
        self.assertEqual(self.point(EXECUTOR_COMPLETED, {EXECUTOR_NAME: "ThreadPoolExecutor"}).value, 1)

    def test_context_is_still_propagated(self):
        # the metrics wrapper must keep the context propagation of the instrumentation
        with futures.ThreadPoolExecutor(max_workers=1) as executor:
            tracer = self.tracer_provider.get_tracer(__name__)
            with tracer.start_as_current_span("parent") as parent:
                span_context = executor.submit(lambda: tracer.start_span("child").parent).result()
        self.assertEqual(span_context.span_id, parent.get_span_context().span_id)


class TestExecutorMetricsDisabled(TestBase):
    def setUp(self):
        super().setUp()
        ThreadingInstrumentor().instrument(meter_provider=self.meter_provider)

    def tearDown(self):
        ThreadingInstrumentor().uninstrument()
        super().tearDown()

    def test_no_metrics_by_default(self):
        with futures.ThreadPoolExecutor(max_workers=1) as executor:
            executor.submit(lambda: None).result()
        self.assertEqual(self.get_sorted_metrics(SCOPE), [])
