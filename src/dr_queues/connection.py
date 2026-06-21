import os
from dataclasses import dataclass
from functools import lru_cache

import pika
from pika.adapters.blocking_connection import (
    BlockingChannel,
    BlockingConnection,
)
from pika.spec import Basic

DEFAULT_AMQP_URL = "amqp://guest:guest@localhost:5672/"


def amqp_url() -> str:
    return os.environ.get("AMQP_URL", DEFAULT_AMQP_URL)


@lru_cache(maxsize=1)
def _parameters() -> pika.URLParameters:
    return pika.URLParameters(amqp_url())


def open_connection() -> BlockingConnection:
    return pika.BlockingConnection(_parameters())


@dataclass
class ChannelSession:
    connection: BlockingConnection
    channel: BlockingChannel

    def close(self) -> None:
        if self.channel.is_open:
            self.channel.close()
        if self.connection.is_open:
            self.connection.close()


def open_session() -> ChannelSession:
    connection = open_connection()
    return ChannelSession(connection=connection, channel=connection.channel())


def declare_durable_queue(
    channel: BlockingChannel,
    name: str,
) -> None:
    channel.queue_declare(queue=name, durable=True)


def delivery_tag(method: Basic.Deliver | Basic.GetOk) -> int:
    tag = method.delivery_tag
    if tag is None:
        msg = "Missing delivery tag on message."
        raise RuntimeError(msg)
    return tag
