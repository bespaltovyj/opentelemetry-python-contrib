# Copyright The OpenTelemetry Authors
# SPDX-License-Identifier: Apache-2.0

import asyncio

import aiohttp
import aiohttp.test_utils
import aiohttp.web

from opentelemetry.instrumentation import aiohttp_client
from opentelemetry.instrumentation._semconv import _StabilityMode
from opentelemetry.sdk.metrics.export import (
    HistogramDataPoint,
    NumberDataPoint,
)
from opentelemetry.semconv._incubating.attributes.http_attributes import (
    HTTP_CONNECTION_STATE,
)
from opentelemetry.semconv._incubating.metrics.http_metrics import (
    HTTP_CLIENT_ACTIVE_REQUESTS,
    HTTP_CLIENT_OPEN_CONNECTIONS,
    HTTP_CLIENT_REQUEST_BODY_SIZE,
    HTTP_CLIENT_RESPONSE_BODY_SIZE,
)
from opentelemetry.semconv.attributes.http_attributes import (
    HTTP_REQUEST_METHOD,
    HTTP_RESPONSE_STATUS_CODE,
)
from opentelemetry.semconv.attributes.server_attributes import (
    SERVER_ADDRESS,
    SERVER_PORT,
)
from opentelemetry.semconv.attributes.url_attributes import URL_SCHEME
from opentelemetry.test.test_base import TestBase

SCOPE = "opentelemetry.instrumentation.aiohttp_client"
RESPONSE_BODY = b"response body"


class TestAioHttpClientMetrics(TestBase):
    def _run(self, sem_conv_opt_in_mode, client_calls):
        """Starts a test server and runs ``client_calls(client)`` with an instrumented client.

        The metrics are collected while the client session is still open."""
        trace_config = aiohttp_client.create_trace_config(
            meter_provider=self.meter_provider,
            sem_conv_opt_in_mode=sem_conv_opt_in_mode,
        )

        async def handler(request):
            await request.read()
            return aiohttp.web.Response(body=RESPONSE_BODY)

        async def run():
            app = aiohttp.web.Application()
            app.add_routes([aiohttp.web.get("/test", handler), aiohttp.web.post("/test", handler)])
            async with aiohttp.test_utils.TestServer(app) as server:
                async with aiohttp.test_utils.TestClient(server, trace_configs=[trace_config]) as client:
                    await client_calls(client)
                    metrics = self.get_sorted_metrics(SCOPE)
                    return server.host, server.port, metrics

        return asyncio.run(run())

    @staticmethod
    def _by_name(metrics):
        return {metric.name: metric for metric in metrics}

    def test_body_sizes(self):
        async def calls(client):
            async with client.post("/test", data=b"0123456789") as response:
                await response.read()

        host, port, metrics = self._run(_StabilityMode.HTTP, calls)
        metrics = self._by_name(metrics)
        expected_attributes = {
            HTTP_REQUEST_METHOD: "POST",
            HTTP_RESPONSE_STATUS_CODE: 200,
            SERVER_ADDRESS: host,
            SERVER_PORT: port,
        }
        for name, size in (
            (HTTP_CLIENT_REQUEST_BODY_SIZE, 10),
            (HTTP_CLIENT_RESPONSE_BODY_SIZE, len(RESPONSE_BODY)),
        ):
            self.assertEqual(metrics[name].unit, "By")
            (point,) = metrics[name].data.data_points
            self.assertIsInstance(point, HistogramDataPoint)
            self.assertEqual(point.count, 1)
            self.assertEqual(point.sum, size)
            self.assertEqual(dict(point.attributes), expected_attributes)

    def test_chunked_request_body(self):
        async def body():
            yield b"01234"
            yield b"56789abc"

        async def calls(client):
            async with client.post("/test", data=body()) as response:
                await response.read()

        _, _, metrics = self._run(_StabilityMode.HTTP, calls)
        (point,) = self._by_name(metrics)[HTTP_CLIENT_REQUEST_BODY_SIZE].data.data_points
        self.assertEqual(point.sum, 13)

    def test_request_without_body(self):
        async def calls(client):
            async with client.get("/test") as response:
                await response.read()

        _, _, metrics = self._run(_StabilityMode.HTTP, calls)
        metrics = self._by_name(metrics)
        self.assertNotIn(HTTP_CLIENT_REQUEST_BODY_SIZE, metrics)
        self.assertIn(HTTP_CLIENT_RESPONSE_BODY_SIZE, metrics)

    def test_active_requests(self):
        observed = []

        async def calls(client):
            async with client.get("/test") as response:
                observed.append(self._by_name(self.get_sorted_metrics(SCOPE))[HTTP_CLIENT_ACTIVE_REQUESTS])
                await response.read()

        host, port, metrics = self._run(_StabilityMode.HTTP, calls)
        attributes = {HTTP_REQUEST_METHOD: "GET", SERVER_ADDRESS: host, SERVER_PORT: port}
        # the request ends once the response headers are received
        (point,) = observed[0].data.data_points
        self.assertEqual(point.value, 0)
        (point,) = self._by_name(metrics)[HTTP_CLIENT_ACTIVE_REQUESTS].data.data_points
        self.assertIsInstance(point, NumberDataPoint)
        self.assertEqual(point.value, 0)
        self.assertEqual(dict(point.attributes), attributes)

    def test_open_connections(self):
        async def calls(client):
            async with client.get("/test") as response:
                await response.read()

        host, port, metrics = self._run(_StabilityMode.HTTP, calls)
        points = {
            point.attributes[HTTP_CONNECTION_STATE]: point
            for point in self._by_name(metrics)[HTTP_CLIENT_OPEN_CONNECTIONS].data.data_points
        }
        # the connection was released to the pool after the response was read
        self.assertEqual(points["idle"].value, 1)
        self.assertEqual(
            dict(points["idle"].attributes),
            {
                HTTP_CONNECTION_STATE: "idle",
                SERVER_ADDRESS: host,
                SERVER_PORT: port,
                URL_SCHEME: "http",
            },
        )

    def test_no_new_metrics_with_old_semconv(self):
        async def calls(client):
            async with client.post("/test", data=b"body") as response:
                await response.read()

        _, _, metrics = self._run(_StabilityMode.DEFAULT, calls)
        names = set(self._by_name(metrics))
        for name in (
            HTTP_CLIENT_ACTIVE_REQUESTS,
            HTTP_CLIENT_OPEN_CONNECTIONS,
            HTTP_CLIENT_REQUEST_BODY_SIZE,
            HTTP_CLIENT_RESPONSE_BODY_SIZE,
        ):
            self.assertNotIn(name, names)
