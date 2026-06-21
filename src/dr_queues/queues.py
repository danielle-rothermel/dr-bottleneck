from typing import ClassVar

from pydantic import BaseModel, computed_field

from dr_queues.connection import (
    ChannelSession,
    PikaBlockingChannel,
    PikaDeliveryMode,
)


class StageQueues(BaseModel):
    model_config = {"arbitrary_types_allowed": True}
    DEFAULT_PENDING_ROLE: ClassVar[str] = "pending"
    DEFAULT_COMPLETED_ROLE: ClassVar[str] = "completed"

    prefix: str
    delivery_mode: PikaDeliveryMode
    pending_role: str | None = None
    completed_role: str | None = None

    @classmethod
    def get_queue_name(cls, prefix: str, role: str) -> str:
        return f"{prefix}.{role}"

    @computed_field
    @property
    def pending_name(self) -> str:
        role_name = self.pending_role or self.DEFAULT_PENDING_ROLE
        return self.get_queue_name(self.prefix, role_name)

    @computed_field
    @property
    def completed_name(self) -> str:
        role_name = self.completed_role or self.DEFAULT_COMPLETED_ROLE
        return self.get_queue_name(self.prefix, role_name)

    def declare_queues(
        self,
        *,
        channel: PikaBlockingChannel | None = None,
    ):
        build_queue_session, channel = ChannelSession.ensure_channel(
            channel=channel,
            delivery_mode=self.delivery_mode,
        )
        try:
            ChannelSession.declare_durable_queue(
                queue_name=self.pending_name,
                channel=channel,
                delivery_mode=self.delivery_mode,
            )
            if self.completed_name != self.pending_name:
                ChannelSession.declare_durable_queue(
                    queue_name=self.completed_name,
                    channel=channel,
                    delivery_mode=self.delivery_mode,
                )
        finally:
            if build_queue_session is not None:
                build_queue_session.close()


def build_stage_queues(
    *,
    prefix: str,
    pending_role: str | None = None,
    completed_role: str | None = None,
    delivery_mode: PikaDeliveryMode = PikaDeliveryMode.PERSISTENT,
) -> StageQueues:
    stage_queues = StageQueues(
        prefix=prefix,
        pending_role=pending_role,
        completed_role=completed_role,
        delivery_mode=delivery_mode,
    )
    stage_queues.declare_queues()
    return stage_queues
