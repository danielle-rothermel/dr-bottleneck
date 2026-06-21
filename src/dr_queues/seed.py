from dr_queues.connection import open_session
from dr_queues.drain import publish_job
from dr_queues.models import JobEnvelope


def seed_jobs(queue_name: str, jobs: list[JobEnvelope]) -> None:
    session = open_session()
    try:
        for job in jobs:
            publish_job(session.channel, queue_name, job.to_json())
    finally:
        session.close()
