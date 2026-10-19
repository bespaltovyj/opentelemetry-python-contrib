# Copyright The OpenTelemetry Authors
# SPDX-License-Identifier: Apache-2.0

"""Database metrics, reported with the ``database`` semantic convention opt-in."""

from __future__ import annotations

import weakref
from collections.abc import Iterable
from timeit import default_timer
from typing import Any

from opentelemetry.metrics import CallbackOptions, Meter, Observation
from opentelemetry.semconv._incubating.attributes.db_attributes import (
    DB_CLIENT_CONNECTION_POOL_NAME,
    DB_CLIENT_CONNECTION_STATE,
    DbClientConnectionStateValues,
    DbSystemNameValues,
)
from opentelemetry.semconv._incubating.metrics.db_metrics import (
    DB_CLIENT_CONNECTION_COUNT,
    DB_CLIENT_CONNECTION_MAX,
    create_db_client_operation_duration,
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

# redis-py uses 2 ** 31 as the max_connections of an unbounded pool
_UNBOUNDED_MAX_CONNECTIONS = 2**31


def _pipeline_operation_name(command_stack: list[Any]) -> str:
    """Returns ``BATCH <command>`` if all the commands are the same, ``BATCH`` otherwise."""
    names = set()
    for command in command_stack:
        args = command.args if hasattr(command, "args") else command[0]
        if args:
            names.add(str(args[0]).upper())
    if len(names) == 1:
        return f"BATCH {names.pop()}"
    return "BATCH"


def _connection_attributes(instance: Any) -> tuple[dict[str, str | int], str] | None:
    """Returns the connection attributes and the pool name of the instance, if it has a pool."""
    pool = getattr(instance, "connection_pool", None)
    connection_kwargs = getattr(pool, "connection_kwargs", None)
    if connection_kwargs is None:
        return None
    db = connection_kwargs.get("db")
    namespace = str(0 if db is None else db)
    attributes: dict[str, str | int] = {
        DB_SYSTEM_NAME: DbSystemNameValues.REDIS.value,
        DB_NAMESPACE: namespace,
    }
    if "path" in connection_kwargs:
        address = connection_kwargs.get("path", "")
        attributes[SERVER_ADDRESS] = address
        pool_name = f"{address}/{namespace}"
    else:
        address = connection_kwargs.get("host", "localhost")
        port = connection_kwargs.get("port", 6379)
        attributes[SERVER_ADDRESS] = address
        attributes[SERVER_PORT] = int(port)
        pool_name = f"{address}:{port}/{namespace}"
    return attributes, pool_name


def _pool_connection_counts(pool: Any) -> tuple[int, int] | None:
    """Returns the number of idle and used connections of a redis-py connection pool."""
    connections = getattr(pool, "_connections", None)
    queue = getattr(getattr(pool, "pool", None), "queue", None)
    if connections is not None and queue is not None:
        # sync BlockingConnectionPool: the queue holds the idle connections and None placeholders
        idle = sum(1 for connection in list(queue) if connection is not None)
        return idle, max(len(connections) - idle, 0)
    available = getattr(pool, "_available_connections", None)
    in_use = getattr(pool, "_in_use_connections", None)
    if available is None or in_use is None:
        return None
    return len(available), len(in_use)


class _RedisMetrics:
    def __init__(self, meter: Meter) -> None:
        self._operation_duration = create_db_client_operation_duration(meter)
        self._pools: weakref.WeakKeyDictionary[Any, str] = weakref.WeakKeyDictionary()
        meter.create_observable_up_down_counter(
            name=DB_CLIENT_CONNECTION_COUNT,
            callbacks=[self._observe_count],
            unit="{connection}",
            description="The number of connections that are currently in state described by the `state` attribute.",
        )
        meter.create_observable_up_down_counter(
            name=DB_CLIENT_CONNECTION_MAX,
            callbacks=[self._observe_max],
            unit="{connection}",
            description="The maximum number of open connections allowed.",
        )

    def record_operation(
        self,
        start: float,
        instance: Any,
        operation_name: str | None,
        error: BaseException | None,
    ) -> None:
        connection = _connection_attributes(instance)
        if connection is None:
            attributes: dict[str, str | int] = {DB_SYSTEM_NAME: DbSystemNameValues.REDIS.value}
        else:
            attributes, pool_name = connection
            attributes = dict(attributes)
            self._pools.setdefault(instance.connection_pool, pool_name)
        if operation_name:
            attributes[DB_OPERATION_NAME] = operation_name
        if error is not None:
            attributes[ERROR_TYPE] = type(error).__qualname__
        self._operation_duration.record(max(default_timer() - start, 0), attributes)

    def clear(self) -> None:
        self._pools.clear()

    def _observe_count(self, options: CallbackOptions) -> Iterable[Observation]:
        for pool, pool_name in list(self._pools.items()):
            counts = _pool_connection_counts(pool)
            if counts is None:
                continue
            for state, value in zip(
                (DbClientConnectionStateValues.IDLE, DbClientConnectionStateValues.USED),
                counts,
            ):
                yield Observation(
                    value,
                    {
                        DB_CLIENT_CONNECTION_POOL_NAME: pool_name,
                        DB_CLIENT_CONNECTION_STATE: state.value,
                    },
                )

    def _observe_max(self, options: CallbackOptions) -> Iterable[Observation]:
        for pool, pool_name in list(self._pools.items()):
            max_connections = getattr(pool, "max_connections", None)
            if isinstance(max_connections, int) and max_connections < _UNBOUNDED_MAX_CONNECTIONS:
                yield Observation(max_connections, {DB_CLIENT_CONNECTION_POOL_NAME: pool_name})
