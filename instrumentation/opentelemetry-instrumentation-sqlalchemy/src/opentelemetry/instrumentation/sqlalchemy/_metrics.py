# Copyright The OpenTelemetry Authors
# SPDX-License-Identifier: Apache-2.0

"""Connection pool and operation metrics following the database semantic conventions.

These metrics are only reported when the ``database`` (or ``database/dup``)
semantic convention stability opt-in is enabled.
"""

from __future__ import annotations

import weakref
from collections.abc import Callable, Iterable
from timeit import default_timer
from typing import TYPE_CHECKING, Any

from sqlalchemy import exc
from sqlalchemy.pool import QueuePool

from opentelemetry.metrics import CallbackOptions, Meter, Observation
from opentelemetry.semconv._incubating.attributes.db_attributes import (
    DB_CLIENT_CONNECTION_POOL_NAME,
    DB_CLIENT_CONNECTION_STATE,
    DbClientConnectionStateValues,
)
from opentelemetry.semconv._incubating.metrics.db_metrics import (
    DB_CLIENT_CONNECTION_IDLE_MAX,
    DB_CLIENT_CONNECTION_MAX,
    create_db_client_connection_count,
    create_db_client_connection_create_time,
    create_db_client_connection_pending_requests,
    create_db_client_connection_timeouts,
    create_db_client_connection_use_time,
    create_db_client_connection_wait_time,
    create_db_client_operation_duration,
)

if TYPE_CHECKING:
    from sqlalchemy.engine import Engine


class _ConnectionPoolMetrics:
    def __init__(self, meter: Meter) -> None:
        self.connection_count = create_db_client_connection_count(meter)
        self.pending_requests = create_db_client_connection_pending_requests(meter)
        self.timeouts = create_db_client_connection_timeouts(meter)
        self.create_time = create_db_client_connection_create_time(meter)
        self.wait_time = create_db_client_connection_wait_time(meter)
        self.use_time = create_db_client_connection_use_time(meter)
        self.operation_duration = create_db_client_operation_duration(meter)
        self._engines: weakref.WeakKeyDictionary[Engine, str] = weakref.WeakKeyDictionary()
        meter.create_observable_up_down_counter(
            name=DB_CLIENT_CONNECTION_MAX,
            callbacks=[self._observe_max],
            unit="{connection}",
            description="The maximum number of open connections allowed.",
        )
        meter.create_observable_up_down_counter(
            name=DB_CLIENT_CONNECTION_IDLE_MAX,
            callbacks=[self._observe_idle_max],
            unit="{connection}",
            description="The maximum number of idle open connections allowed.",
        )

    def register(self, engine: Engine, pool_name: str) -> None:
        self._engines[engine] = pool_name

    def pool_name(self, engine: Engine) -> str | None:
        return self._engines.get(engine)

    def clear(self) -> None:
        self._engines.clear()

    @staticmethod
    def pool_attributes(pool_name: str) -> dict[str, str]:
        return {DB_CLIENT_CONNECTION_POOL_NAME: pool_name}

    def add_connection_count(self, pool_name: str, used: bool, value: int) -> None:
        state = DbClientConnectionStateValues.USED if used else DbClientConnectionStateValues.IDLE
        self.connection_count.add(
            value,
            {
                DB_CLIENT_CONNECTION_POOL_NAME: pool_name,
                DB_CLIENT_CONNECTION_STATE: state.value,
            },
        )

    def measure_wait(
        self,
        pool_name: str,
        func: Callable[..., Any],
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
    ) -> Any:
        """Calls ``func`` (which checks a connection out of the pool) recording
        the pending requests, the wait time and the timeouts."""
        attributes = self.pool_attributes(pool_name)
        self.pending_requests.add(1, attributes)
        start = default_timer()
        try:
            return func(*args, **kwargs)
        except exc.TimeoutError:
            self.timeouts.add(1, attributes)
            raise
        finally:
            self.pending_requests.add(-1, attributes)
            self.wait_time.record(max(default_timer() - start, 0), attributes)

    def _queue_pools(self) -> Iterable[tuple[QueuePool, str]]:
        for engine, pool_name in list(self._engines.items()):
            pool = getattr(engine, "pool", None)
            if isinstance(pool, QueuePool):
                yield pool, pool_name

    def _observe_max(self, options: CallbackOptions) -> Iterable[Observation]:
        for pool, pool_name in self._queue_pools():
            # a negative max_overflow means that the pool size is unbounded
            max_overflow = getattr(pool, "_max_overflow", None)
            if isinstance(max_overflow, int) and max_overflow >= 0:
                yield Observation(pool.size() + max_overflow, self.pool_attributes(pool_name))

    def _observe_idle_max(self, options: CallbackOptions) -> Iterable[Observation]:
        for pool, pool_name in self._queue_pools():
            yield Observation(pool.size(), self.pool_attributes(pool_name))
