# Copyright The OpenTelemetry Authors
# SPDX-License-Identifier: Apache-2.0

import asyncio
from unittest import mock

import sqlalchemy
from sqlalchemy import exc, text
from sqlalchemy.pool import NullPool, QueuePool

from opentelemetry.instrumentation._semconv import (
    OTEL_SEMCONV_STABILITY_OPT_IN,
    _OpenTelemetrySemanticConventionStability,
)
from opentelemetry.instrumentation.sqlalchemy import SQLAlchemyInstrumentor
from opentelemetry.sdk.metrics.export import (
    HistogramDataPoint,
    NumberDataPoint,
)
from opentelemetry.semconv._incubating.attributes.db_attributes import (
    DB_CLIENT_CONNECTION_POOL_NAME,
    DB_CLIENT_CONNECTION_STATE,
)
from opentelemetry.semconv._incubating.metrics.db_metrics import (
    DB_CLIENT_CONNECTION_COUNT,
    DB_CLIENT_CONNECTION_CREATE_TIME,
    DB_CLIENT_CONNECTION_IDLE_MAX,
    DB_CLIENT_CONNECTION_MAX,
    DB_CLIENT_CONNECTION_PENDING_REQUESTS,
    DB_CLIENT_CONNECTION_TIMEOUTS,
    DB_CLIENT_CONNECTION_USE_TIME,
    DB_CLIENT_CONNECTION_WAIT_TIME,
    DB_CLIENT_CONNECTIONS_USAGE,
    DB_CLIENT_OPERATION_DURATION,
)
from opentelemetry.semconv.attributes.db_attributes import (
    DB_NAMESPACE,
    DB_OPERATION_NAME,
    DB_SYSTEM_NAME,
)
from opentelemetry.semconv.attributes.error_attributes import ERROR_TYPE
from opentelemetry.test.test_base import TestBase

SCOPE = "opentelemetry.instrumentation.sqlalchemy"
POOL_NAME = "pool_test_name"
POOL_ATTRIBUTES = {DB_CLIENT_CONNECTION_POOL_NAME: POOL_NAME}


class _PoolMetricsTestBase(TestBase):
    opt_in = "database"

    def setUp(self):
        super().setUp()
        self._env_patch = mock.patch.dict("os.environ", {OTEL_SEMCONV_STABILITY_OPT_IN: self.opt_in})
        self._env_patch.start()
        _OpenTelemetrySemanticConventionStability._initialized = False
        SQLAlchemyInstrumentor().instrument(
            tracer_provider=self.tracer_provider,
            meter_provider=self.meter_provider,
        )

    def tearDown(self):
        super().tearDown()
        SQLAlchemyInstrumentor().uninstrument()
        self._env_patch.stop()
        _OpenTelemetrySemanticConventionStability._initialized = False

    @staticmethod
    def create_engine(**kwargs):
        kwargs.setdefault("poolclass", QueuePool)
        kwargs.setdefault("pool_size", 2)
        kwargs.setdefault("max_overflow", 1)
        return sqlalchemy.create_engine(
            "sqlite:///:memory:",
            pool_logging_name=POOL_NAME,
            # None removes a pool option which is not supported by the pool class
            **{key: value for key, value in kwargs.items() if value is not None},
        )

    def metrics_by_name(self):
        return {metric.name: metric for metric in self.get_sorted_metrics(SCOPE)}

    def data_points(self, name):
        metric = self.metrics_by_name().get(name)
        return [] if metric is None else list(metric.data.data_points)

    def assert_number(self, name, attributes, value):
        points = [point for point in self.data_points(name) if dict(point.attributes) == attributes]
        self.assertEqual(len(points), 1, f"{name}: {self.data_points(name)}")
        self.assertIsInstance(points[0], NumberDataPoint)
        self.assertIsInstance(points[0].value, int)
        self.assertEqual(points[0].value, value)

    def assert_histogram(self, name, attributes, count):
        points = [point for point in self.data_points(name) if dict(point.attributes) == attributes]
        self.assertEqual(len(points), 1, f"{name}: {self.data_points(name)}")
        self.assertIsInstance(points[0], HistogramDataPoint)
        self.assertEqual(points[0].count, count)
        self.assertGreaterEqual(points[0].sum, 0)
        self.assertEqual(self.metrics_by_name()[name].unit, "s")


class TestSqlalchemyPoolMetrics(_PoolMetricsTestBase):
    def assert_count(self, idle, used):
        for state, value in (("idle", idle), ("used", used)):
            self.assert_number(
                DB_CLIENT_CONNECTION_COUNT,
                {**POOL_ATTRIBUTES, DB_CLIENT_CONNECTION_STATE: state},
                value,
            )

    def test_connection_count(self):
        engine = self.create_engine()
        with engine.connect():
            with engine.connect():
                self.assert_count(idle=0, used=2)
            self.assert_count(idle=1, used=1)
        self.assert_count(idle=2, used=0)
        # the old experimental metric is not reported with the database opt-in
        self.assertNotIn(DB_CLIENT_CONNECTIONS_USAGE, self.metrics_by_name())

    def test_pool_limits(self):
        engine = self.create_engine(pool_size=3, max_overflow=2)
        with engine.connect():
            pass
        self.assert_number(DB_CLIENT_CONNECTION_MAX, POOL_ATTRIBUTES, 5)
        self.assert_number(DB_CLIENT_CONNECTION_IDLE_MAX, POOL_ATTRIBUTES, 3)

    def test_unbounded_pool_has_no_max(self):
        engine = self.create_engine(max_overflow=-1)
        with engine.connect():
            pass
        self.assertEqual(self.data_points(DB_CLIENT_CONNECTION_MAX), [])
        self.assert_number(DB_CLIENT_CONNECTION_IDLE_MAX, POOL_ATTRIBUTES, 2)

    def test_non_queue_pool_has_no_limits(self):
        engine = self.create_engine(poolclass=NullPool, pool_size=None, max_overflow=None)
        with engine.connect():
            pass
        self.assertEqual(self.data_points(DB_CLIENT_CONNECTION_MAX), [])
        self.assertEqual(self.data_points(DB_CLIENT_CONNECTION_IDLE_MAX), [])
        self.assert_histogram(DB_CLIENT_CONNECTION_WAIT_TIME, POOL_ATTRIBUTES, 1)

    def test_connection_times(self):
        engine = self.create_engine()
        with engine.connect():
            pass
        # the second checkout reuses the idle connection
        with engine.connect():
            pass
        self.assert_histogram(DB_CLIENT_CONNECTION_CREATE_TIME, POOL_ATTRIBUTES, 1)
        self.assert_histogram(DB_CLIENT_CONNECTION_WAIT_TIME, POOL_ATTRIBUTES, 2)
        self.assert_histogram(DB_CLIENT_CONNECTION_USE_TIME, POOL_ATTRIBUTES, 2)
        self.assert_number(DB_CLIENT_CONNECTION_PENDING_REQUESTS, POOL_ATTRIBUTES, 0)

    def test_connection_timeout(self):
        engine = self.create_engine(pool_size=1, max_overflow=0, pool_timeout=0.01)
        with engine.connect():
            with self.assertRaises(exc.TimeoutError):
                engine.connect()
        self.assert_number(DB_CLIENT_CONNECTION_TIMEOUTS, POOL_ATTRIBUTES, 1)
        self.assert_number(DB_CLIENT_CONNECTION_PENDING_REQUESTS, POOL_ATTRIBUTES, 0)
        self.assert_histogram(DB_CLIENT_CONNECTION_WAIT_TIME, POOL_ATTRIBUTES, 2)

    def test_pool_metrics_survive_dispose(self):
        engine = self.create_engine()
        with engine.connect():
            pass
        engine.dispose()
        with engine.connect():
            pass
        self.assert_histogram(DB_CLIENT_CONNECTION_WAIT_TIME, POOL_ATTRIBUTES, 2)

    def test_operation_duration(self):
        engine = self.create_engine()
        with engine.connect() as connection:
            connection.execute(text("SELECT 1 + 1;")).fetchall()
        self.assert_histogram(
            DB_CLIENT_OPERATION_DURATION,
            {
                DB_SYSTEM_NAME: "sqlite",
                DB_NAMESPACE: ":memory:",
                DB_OPERATION_NAME: "SELECT",
            },
            1,
        )

    def test_operation_duration_error(self):
        engine = self.create_engine()
        with engine.connect() as connection:
            with self.assertRaises(exc.OperationalError):
                connection.execute(text("SELECT * FROM missing_table"))
        self.assert_histogram(
            DB_CLIENT_OPERATION_DURATION,
            {
                DB_SYSTEM_NAME: "sqlite",
                DB_NAMESPACE: ":memory:",
                DB_OPERATION_NAME: "SELECT",
                ERROR_TYPE: "OperationalError",
            },
            1,
        )

    def test_async_engine(self):
        async def run():
            # pylint: disable-next=import-outside-toplevel
            from sqlalchemy.ext.asyncio import create_async_engine  # noqa: PLC0415

            engine = create_async_engine(
                "sqlite+aiosqlite:///:memory:",
                pool_logging_name=POOL_NAME,
            )
            async with engine.connect() as connection:
                await connection.execute(text("SELECT 1 + 1;"))
            await engine.dispose()

        asyncio.run(run())
        self.assert_histogram(DB_CLIENT_CONNECTION_WAIT_TIME, POOL_ATTRIBUTES, 1)
        self.assert_histogram(DB_CLIENT_CONNECTION_USE_TIME, POOL_ATTRIBUTES, 1)
        self.assert_histogram(
            DB_CLIENT_OPERATION_DURATION,
            {
                DB_SYSTEM_NAME: "sqlite",
                DB_NAMESPACE: ":memory:",
                DB_OPERATION_NAME: "SELECT",
            },
            1,
        )

    def test_uninstrument(self):
        SQLAlchemyInstrumentor().uninstrument()
        engine = self.create_engine()
        with engine.connect():
            pass
        self.assertEqual(self.data_points(DB_CLIENT_CONNECTION_WAIT_TIME), [])
        self.assertEqual(self.data_points(DB_CLIENT_CONNECTION_COUNT), [])


class TestSqlalchemyPoolMetricsDup(_PoolMetricsTestBase):
    opt_in = "database/dup"

    def test_old_and_new_connection_count(self):
        engine = self.create_engine()
        with engine.connect():
            self.assert_number(
                DB_CLIENT_CONNECTIONS_USAGE,
                {"pool.name": POOL_NAME, "state": "used"},
                1,
            )
            self.assert_number(
                DB_CLIENT_CONNECTION_COUNT,
                {**POOL_ATTRIBUTES, DB_CLIENT_CONNECTION_STATE: "used"},
                1,
            )


class TestSqlalchemyPoolMetricsDefault(_PoolMetricsTestBase):
    opt_in = ""

    def test_only_old_metric(self):
        engine = self.create_engine()
        with engine.connect() as connection:
            connection.execute(text("SELECT 1 + 1;")).fetchall()
        self.assertEqual(list(self.metrics_by_name()), [DB_CLIENT_CONNECTIONS_USAGE])
