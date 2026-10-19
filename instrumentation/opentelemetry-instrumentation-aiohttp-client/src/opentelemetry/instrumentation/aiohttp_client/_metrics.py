# Copyright The OpenTelemetry Authors
# SPDX-License-Identifier: Apache-2.0

"""Connection pool metrics of the ``aiohttp`` connectors used by instrumented sessions."""

from __future__ import annotations

import weakref
from collections import defaultdict
from collections.abc import Iterable, Sized
from typing import Any, cast

import aiohttp

from opentelemetry.metrics import CallbackOptions, Meter, Observation
from opentelemetry.semconv._incubating.attributes.http_attributes import (
    HTTP_CONNECTION_STATE,
    HttpConnectionStateValues,
)
from opentelemetry.semconv._incubating.metrics.http_metrics import (
    HTTP_CLIENT_OPEN_CONNECTIONS,
)
from opentelemetry.semconv.attributes.server_attributes import (
    SERVER_ADDRESS,
    SERVER_PORT,
)
from opentelemetry.semconv.attributes.url_attributes import URL_SCHEME


def _connection_key_attributes(key: Any) -> tuple[str, int, str] | None:
    host = getattr(key, "host", None)
    port = getattr(key, "port", None)
    if host is None or port is None:
        return None
    scheme = "https" if getattr(key, "is_ssl", False) else "http"
    return host, port, scheme


class _ConnectionPoolObserver:
    """Observes the idle and active connections of the registered connectors.

    The connection pool state is only available through private attributes
    of :class:`aiohttp.BaseConnector` (``_conns`` and ``_acquired_per_host``),
    connectors without them are ignored.
    """

    def __init__(self, meter: Meter) -> None:
        self._connectors: weakref.WeakSet[aiohttp.BaseConnector] = weakref.WeakSet()
        meter.create_observable_up_down_counter(
            name=HTTP_CLIENT_OPEN_CONNECTIONS,
            callbacks=[self._observe],
            unit="{connection}",
            description="Number of outbound HTTP connections that are currently active or idle on the client.",
        )

    def add(self, connector: aiohttp.BaseConnector | None) -> None:
        if connector is not None:
            self._connectors.add(connector)

    def _observe(self, options: CallbackOptions) -> Iterable[Observation]:
        counts: defaultdict[tuple[str, int, str, str], int] = defaultdict(int)
        for connector in list(self._connectors):
            if connector.closed:
                continue
            states = (
                (HttpConnectionStateValues.IDLE, getattr(connector, "_conns", None)),
                (HttpConnectionStateValues.ACTIVE, getattr(connector, "_acquired_per_host", None)),
            )
            for state, connections_per_key in states:
                if not isinstance(connections_per_key, dict):
                    continue
                typed_connections_per_key = cast(dict[Any, Sized], connections_per_key)
                for key, connections in list(typed_connections_per_key.items()):
                    key_attributes = _connection_key_attributes(key)
                    if key_attributes is not None:
                        counts[(*key_attributes, state.value)] += len(connections)
        for (host, port, scheme, state), value in counts.items():
            yield Observation(
                value,
                {
                    HTTP_CONNECTION_STATE: state,
                    SERVER_ADDRESS: host,
                    SERVER_PORT: port,
                    URL_SCHEME: scheme,
                },
            )


_POOL_OBSERVERS: weakref.WeakKeyDictionary[Meter, _ConnectionPoolObserver] = weakref.WeakKeyDictionary()


def get_connection_pool_observer(meter: Meter) -> _ConnectionPoolObserver:
    """Returns the observer of the meter, so that the observable instrument is created once."""
    observer = _POOL_OBSERVERS.get(meter)
    if observer is None:
        observer = _ConnectionPoolObserver(meter)
        _POOL_OBSERVERS[meter] = observer
    return observer
