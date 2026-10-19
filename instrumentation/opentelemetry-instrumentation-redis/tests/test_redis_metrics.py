# Copyright The OpenTelemetry Authors
# SPDX-License-Identifier: Apache-2.0

from unittest import IsolatedAsyncioTestCase, mock

import fakeredis
import redis
from fakeredis.aioredis import FakeRedis

from opentelemetry.instrumentation._semconv import (
    OTEL_SEMCONV_STABILITY_OPT_IN,
    _OpenTelemetrySemanticConventionStability,
)
from opentelemetry.instrumentation.redis import RedisInstrumentor
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
    DB_CLIENT_CONNECTION_MAX,
    DB_CLIENT_OPERATION_DURATION,
)
from opentelemetry.semconv.attributes.db_attributes import (
    DB_NAMESPACE,
    DB_OPERATION_NAME,
    DB_SYSTEM_NAME,
)
from opentelemetry.semconv.attributes.error_attributes import ERROR_TYPE
from opentelemetry.semconv.attributes.server_attributes import (
    SERVER_ADDRESS,
    SERVER_PORT,
)
from opentelemetry.test.test_base import TestBase


def _connection_attributes(client):
    # fakeredis uses a random host name for every fake server
    host = client.connection_pool.connection_kwargs["host"]
    return {
        DB_SYSTEM_NAME: "redis",
        DB_NAMESPACE: "0",
        SERVER_ADDRESS: host,
        SERVER_PORT: 6379,
    }


def _pool_name(client):
    return f"{client.connection_pool.connection_kwargs['host']}:6379/0"


class _RedisMetricsTestBase(TestBase):
    opt_in = "database"

    def setUp(self):
        super().setUp()
        self._env_patch = mock.patch.dict("os.environ", {OTEL_SEMCONV_STABILITY_OPT_IN: self.opt_in})
        self._env_patch.start()
        _OpenTelemetrySemanticConventionStability._initialized = False
        RedisInstrumentor().instrument(
            tracer_provider=self.tracer_provider,
            meter_provider=self.meter_provider,
        )

    def tearDown(self):
        super().tearDown()
        RedisInstrumentor().uninstrument()
        self._env_patch.stop()
        _OpenTelemetrySemanticConventionStability._initialized = False

    def points(self, name):
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

    def assert_duration(self, client, operation_name, count=1, error_type=None):
        attributes = {**_connection_attributes(client), DB_OPERATION_NAME: operation_name}
        if error_type is not None:
            attributes[ERROR_TYPE] = error_type
        points = [point for point in self.points(DB_CLIENT_OPERATION_DURATION) if dict(point.attributes) == attributes]
        self.assertEqual(len(points), 1, self.points(DB_CLIENT_OPERATION_DURATION))
        self.assertIsInstance(points[0], HistogramDataPoint)
        self.assertEqual(points[0].count, count)
        self.assertIsInstance(points[0].attributes[SERVER_PORT], int)
        self.assertIsInstance(points[0].attributes[DB_NAMESPACE], str)


class TestRedisMetrics(_RedisMetricsTestBase):
    def test_command_duration(self):
        client = fakeredis.FakeStrictRedis()
        client.set("key", "value")
        client.get("key")
        client.get("key")
        self.assert_duration(client, "SET")
        self.assert_duration(client, "GET", count=2)

    def test_command_error(self):
        client = fakeredis.FakeStrictRedis()
        client.set("key", "value")
        with self.assertRaises(redis.ResponseError):
            client.lpush("key", "value")
        self.assert_duration(client, "LPUSH", error_type="ResponseError")

    def test_pipeline_duration(self):
        client = fakeredis.FakeStrictRedis()
        with client.pipeline() as pipeline:
            pipeline.set("a", 1)
            pipeline.set("b", 2)
            pipeline.execute()
        with client.pipeline() as pipeline:
            pipeline.set("a", 1)
            pipeline.get("a")
            pipeline.execute()
        self.assert_duration(client, "BATCH SET")
        self.assert_duration(client, "BATCH")

    def test_connection_pool(self):
        client = fakeredis.FakeStrictRedis()
        client.set("key", "value")
        points = {
            point.attributes[DB_CLIENT_CONNECTION_STATE]: point for point in self.points(DB_CLIENT_CONNECTION_COUNT)
        }
        self.assertEqual(set(points), {"idle", "used"})
        for state, value in (("idle", 1), ("used", 0)):
            self.assertIsInstance(points[state], NumberDataPoint)
            self.assertEqual(points[state].value, value)
            self.assertEqual(
                dict(points[state].attributes),
                {DB_CLIENT_CONNECTION_POOL_NAME: _pool_name(client), DB_CLIENT_CONNECTION_STATE: state},
            )

    def test_connection_pool_max(self):
        pool = redis.ConnectionPool(
            connection_class=fakeredis.FakeConnection,
            max_connections=7,
            host="localhost",
            server=fakeredis.FakeServer(),
        )
        client = redis.Redis(connection_pool=pool)
        client.set("key", "value")
        points = self.points(DB_CLIENT_CONNECTION_MAX)
        self.assertEqual(len(points), 1)
        self.assertEqual(points[0].value, 7)
        self.assertEqual(dict(points[0].attributes), {DB_CLIENT_CONNECTION_POOL_NAME: _pool_name(client)})

    def test_blocking_connection_pool(self):
        pool = redis.BlockingConnectionPool(
            connection_class=fakeredis.FakeConnection,
            max_connections=3,
            host="localhost",
            server=fakeredis.FakeServer(),
        )
        client = redis.Redis(connection_pool=pool)
        client.set("key", "value")
        counts = {
            point.attributes[DB_CLIENT_CONNECTION_STATE]: point.value
            for point in self.points(DB_CLIENT_CONNECTION_COUNT)
        }
        self.assertEqual(counts, {"idle": 1, "used": 0})
        self.assertEqual([point.value for point in self.points(DB_CLIENT_CONNECTION_MAX)], [3])

    def test_unbounded_pool_has_no_max(self):
        client = fakeredis.FakeStrictRedis()
        client.set("key", "value")
        self.assertEqual(self.points(DB_CLIENT_CONNECTION_MAX), [])

    def test_instrument_client(self):
        RedisInstrumentor().uninstrument()
        client = fakeredis.FakeStrictRedis()
        RedisInstrumentor.instrument_client(client, meter_provider=self.meter_provider)
        try:
            client.get("key")
        finally:
            RedisInstrumentor.uninstrument_client(client)
        self.assert_duration(client, "GET")


class TestRedisMetricsAsync(_RedisMetricsTestBase, IsolatedAsyncioTestCase):
    async def test_command_duration(self):
        client = FakeRedis()
        await client.set("key", "value")
        await client.get("key")
        self.assert_duration(client, "SET")
        self.assert_duration(client, "GET")

    async def test_pipeline_duration(self):
        client = FakeRedis()
        async with client.pipeline() as pipeline:
            await pipeline.set("a", 1)
            await pipeline.set("b", 2)
            await pipeline.execute()
        self.assert_duration(client, "BATCH SET")


class TestRedisMetricsDefault(_RedisMetricsTestBase):
    opt_in = ""

    def test_no_metrics_without_opt_in(self):
        client = fakeredis.FakeStrictRedis()
        client.set("key", "value")
        self.assertIsNone(self.memory_metrics_reader.get_metrics_data())
