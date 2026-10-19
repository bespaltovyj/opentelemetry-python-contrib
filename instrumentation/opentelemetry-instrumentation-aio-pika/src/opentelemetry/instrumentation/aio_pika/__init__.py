# Copyright The OpenTelemetry Authors
# SPDX-License-Identifier: Apache-2.0

"""
Instrument aio_pika to trace RabbitMQ applications.

Usage
-----
Start broker backend

.. code-block:: python

    docker run -p 5672:5672 rabbitmq

Run instrumented task

.. code-block:: python

    import asyncio

    from aio_pika import Message, connect
    from opentelemetry.instrumentation.aio_pika import AioPikaInstrumentor

    AioPikaInstrumentor().instrument()


    async def main() -> None:
        connection = await connect("amqp://guest:guest@localhost/")
        async with connection:
            channel = await connection.channel()
            queue = await channel.declare_queue("hello")
            await channel.default_exchange.publish(Message(b"Hello World!"), routing_key=queue.name)


    if __name__ == "__main__":
        asyncio.run(main())

Metrics
-------

When the ``OTEL_SEMCONV_STABILITY_OPT_IN`` environment variable contains
``messaging`` (or ``messaging/dup``), the following metrics are reported with
the ``messaging.system``, ``messaging.operation.name``,
``messaging.destination.name``, ``server.address``, ``server.port`` and, on
failures, ``error.type`` attributes:

- ``messaging.client.operation.duration`` and ``messaging.client.sent.messages``
  for ``Exchange.publish`` (the destination is the exchange name, or
  ``amq.default`` for the default exchange)
- ``messaging.process.duration`` and ``messaging.client.consumed.messages`` for
  the callbacks registered with ``Queue.consume`` (the destination is the queue
  name)

A custom meter provider can be passed with ``instrument(meter_provider=...)``.

API
---
"""
# pylint: disable=import-error

from .aio_pika_instrumentor import AioPikaInstrumentor
from .version import __version__

__all__ = ["AioPikaInstrumentor", "__version__"]
