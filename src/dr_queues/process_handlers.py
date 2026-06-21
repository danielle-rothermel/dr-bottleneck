from collections.abc import Callable

from dr_queues.job import JobEnvelope
from dr_queues.workflow import WorkflowStep

ProcessHandler = Callable[[JobEnvelope, WorkflowStep], JobEnvelope]

_REGISTRY: dict[str, ProcessHandler] = {}


def register(name: str) -> Callable[[ProcessHandler], ProcessHandler]:
    def decorator(handler: ProcessHandler) -> ProcessHandler:
        _REGISTRY[name] = handler
        return handler

    return decorator


def get_process_handler(name: str) -> ProcessHandler:
    if name not in _REGISTRY:
        msg = f"Unknown process handler: {name}"
        raise ValueError(msg)
    return _REGISTRY[name]
