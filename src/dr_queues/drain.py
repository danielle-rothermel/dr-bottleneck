import json
from collections.abc import Callable
from typing import Any

import pika
from pika.adapters.blocking_connection import BlockingChannel
from pika.spec import Basic, BasicProperties

from dr_queues.connection import declare_durable_queue, delivery_tag
from dr_queues.models import DrainEvent

DRAIN_QUEUE = "dr.drain"


def ensure_drain_queue(channel: BlockingChannel) -> None:
    declare_durable_queue(channel, DRAIN_QUEUE)


def add_to_drain(channel: BlockingChannel, event: DrainEvent) -> None:
    ensure_drain_queue(channel)
    channel.basic_publish(
        exchange="",
        routing_key=DRAIN_QUEUE,
        body=event.to_json(),
        properties=pika.BasicProperties(delivery_mode=2),
    )


def finalize_message(
    channel: BlockingChannel,
    method: Basic.Deliver,
    *,
    drain_payload: DrainEvent,
    publish_fn: Callable[[BlockingChannel], None] | None = None,
) -> None:
    add_to_drain(channel, drain_payload)
    if publish_fn is not None:
        publish_fn(channel)
    channel.basic_ack(delivery_tag=delivery_tag(method))


def publish_job(
    channel: BlockingChannel,
    queue_name: str,
    body: bytes,
) -> None:
    channel.basic_publish(
        exchange="",
        routing_key=queue_name,
        body=body,
        properties=BasicProperties(delivery_mode=2),
    )


def _read_body(body: bytes | None) -> dict[str, Any]:
    if body is None:
        msg = "Drain message body was empty."
        raise RuntimeError(msg)
    return json.loads(body.decode("utf-8"))


def peek_drain(channel: BlockingChannel) -> list[dict[str, Any]]:
    ensure_drain_queue(channel)
    events: list[dict[str, Any]] = []
    payloads: list[tuple[bytes, BasicProperties | None]] = []
    while True:
        method, properties, body = channel.basic_get(
            queue=DRAIN_QUEUE,
            auto_ack=False,
        )
        if method is None:
            break
        if body is None:
            channel.basic_ack(delivery_tag=delivery_tag(method))
            continue
        events.append(_read_body(body))
        payloads.append((body, properties))
        channel.basic_ack(delivery_tag=delivery_tag(method))

    for body, properties in payloads:
        channel.basic_publish(
            exchange="",
            routing_key=DRAIN_QUEUE,
            body=body,
            properties=properties or BasicProperties(delivery_mode=2),
        )
    return events


def dump_drain(channel: BlockingChannel) -> list[dict[str, Any]]:
    ensure_drain_queue(channel)
    events: list[dict[str, Any]] = []
    while True:
        method, _properties, body = channel.basic_get(
            queue=DRAIN_QUEUE,
            auto_ack=True,
        )
        if method is None:
            break
        events.append(_read_body(body))
    return events
