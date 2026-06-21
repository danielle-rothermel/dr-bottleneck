from collections.abc import Callable
from datetime import UTC, datetime
from enum import StrEnum
from typing import TYPE_CHECKING, Any

from pydantic import BaseModel, Field

from dr_queues.connection import (
    ChannelSession,
    PikaBlockingChannel,
    PikaDeliveryMode,
    PikaDeliveryTaggedMethod,
    ReceivedMessage,
    make_delivery_props,
    publish_job,
)
from dr_queues.utils import load_json_body

# TODO: why is this complaining?
if TYPE_CHECKING:
    from pika import BasicProperties


DRAIN_QUEUE = "dr.drain"


class DrainAction(StrEnum):
    PEEK_DRAIN = "peek_drain"
    DUMP_DRAIN = "dump_drain"


class DrainEventKind(StrEnum):
    STAGE_STARTED = "stage_started"
    STAGE_OUTPUT = "stage_output"
    TERMINAL = "terminal"


class DrainEvent(BaseModel):
    run_id: str
    job_id: str
    lane: str
    stage: str
    event: DrainEventKind
    timestamp: str = Field(
        default_factory=lambda: datetime.now(tz=UTC).isoformat(),
    )
    payload: dict[str, Any] = Field(default_factory=dict)

    def to_json(self) -> bytes:
        return self.model_dump_json().encode("utf-8")

    @classmethod
    def from_json(cls, payload: bytes) -> "DrainEvent":
        return cls.model_validate_json(payload)


def ensure_drain_queue(
    *,
    delivery_mode: PikaDeliveryMode,
    channel: PikaBlockingChannel | None = None,
    queue_name: str = DRAIN_QUEUE,
) -> None:
    ChannelSession.ensure_durable_queue(
        queue_name=queue_name,
        channel=channel,
        delivery_mode=delivery_mode,
    )


def add_to_drain(
    channel: PikaBlockingChannel,
    event: DrainEvent,
    *,
    queue_name: str = DRAIN_QUEUE,
    delivery_mode: PikaDeliveryMode = PikaDeliveryMode.PERSISTENT,
) -> None:
    ensure_drain_queue(
        channel=channel,
        queue_name=queue_name,
        delivery_mode=delivery_mode,
    )
    publish_job(
        channel=channel,
        queue_name=queue_name,
        body=event.to_json(),
        properties=make_delivery_props(delivery_mode=delivery_mode),
    )


def peek_drain(
    *,
    channel: PikaBlockingChannel | None = None,
    queue_name: str = DRAIN_QUEUE,
    delivery_mode: PikaDeliveryMode = PikaDeliveryMode.PERSISTENT,
) -> list[dict[str, Any]]:
    drain_session, channel = ChannelSession.ensure_channel(
        channel=channel, delivery_mode=delivery_mode
    )
    ensure_drain_queue(
        channel=channel,
        queue_name=queue_name,
        delivery_mode=delivery_mode,
    )

    events: list[dict[str, Any]] = []
    payloads: list[tuple[bytes, BasicProperties | None]] = []
    has_messages: bool = True
    while has_messages:
        message_obj = ReceivedMessage.from_get_tuple(
            *channel.basic_get(
                queue=queue_name,
                auto_ack=False,
            )
        )
        if not message_obj.has_messages:
            break
        events.append(message_obj.event)
        payloads.append(message_obj.payload)
        channel.basic_ack(delivery_tag=message_obj.delivery_tag)

    # TODO: is the potential delivery mode mismatch if it is set in properties
    # and in peek_drain params an issue?
    default_props = make_delivery_props(delivery_mode=delivery_mode)
    for body, properties in payloads:
        publish_job(
            channel=channel,
            queue_name=queue_name,
            body=body,
            properties=properties or default_props,
        )
    if drain_session is not None:
        drain_session.close()
    return events


def dump_drain(
    channel: PikaBlockingChannel,
    *,
    queue_name: str = DRAIN_QUEUE,
    delivery_mode: PikaDeliveryMode = PikaDeliveryMode.PERSISTENT,
) -> list[dict[str, Any]]:
    ensure_drain_queue(
        channel=channel,
        queue_name=queue_name,
        delivery_mode=delivery_mode,
    )
    events: list[dict[str, Any]] = []
    while True:
        method, _properties, body = channel.basic_get(
            queue=queue_name,
            auto_ack=True,
        )
        if method is None:
            break
        events.append(load_json_body(body))
    return events


def finalize_message(
    channel: PikaBlockingChannel,
    method: PikaDeliveryTaggedMethod,
    *,
    drain_payload: DrainEvent,
    publish_fn: Callable[[PikaBlockingChannel], None] | None = None,
) -> None:
    add_to_drain(channel, drain_payload)
    if publish_fn is not None:
        publish_fn(channel)
    channel.basic_ack(delivery_tag=method.delivery_tag)
