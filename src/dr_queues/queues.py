from dr_queues.connection import declare_durable_queue, open_session
from dr_queues.models import StageQueues


def _queue_name(prefix: str, role: str) -> str:
    return f"{prefix}.{role}"


def build_stage_queues(
    *,
    prefix: str,
    pending: str | None = None,
    completed: str | None = None,
) -> StageQueues:
    pending_name = pending or _queue_name(prefix, "pending")
    completed_name = completed or _queue_name(prefix, "completed")

    session = open_session()
    try:
        declare_durable_queue(session.channel, pending_name)
        if completed_name != pending_name:
            declare_durable_queue(session.channel, completed_name)
    finally:
        session.close()

    return StageQueues(
        prefix=prefix,
        pending_name=pending_name,
        completed_name=completed_name,
    )
