import ast
from collections.abc import Callable
from datetime import UTC, datetime

import zstandard

from dr_queues.models import JobEnvelope, ProcessStepResult, WorkflowStep

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


@register("humaneval_compress_ast")
def humaneval_compress_ast(
    job: JobEnvelope,
    step: WorkflowStep,
) -> JobEnvelope:
    encode_step = step.config.get("encode_step", "encode")
    decode_step = step.config.get("decode_step", "decode")
    zstd_level = int(step.config.get("zstd_level", 22))

    encoded_text = job.step_outputs.get(encode_step, "")
    decoded_text = job.step_outputs.get(decode_step, "")

    encoded_bytes = encoded_text.encode("utf-8")
    encoded_len_raw = len(encoded_bytes)

    compressor = zstandard.ZstdCompressor(level=zstd_level)
    compressed = compressor.compress(encoded_bytes)
    encoded_len_compressed = len(compressed)

    ast_parse_ok = 0
    try:
        ast.parse(decoded_text)
        ast_parse_ok = 1
    except SyntaxError:
        ast_parse_ok = 0

    result = {
        "encoded_len_raw": encoded_len_raw,
        "encoded_len_compressed": encoded_len_compressed,
        "ast_parse_ok": ast_parse_ok,
        "zstd_level": zstd_level,
    }

    job.step_outputs[step.name] = (
        f"raw={encoded_len_raw} compressed={encoded_len_compressed} "
        f"ast={ast_parse_ok}"
    )
    job.step_process_results[step.name] = ProcessStepResult(
        step_index=job.step_index,
        name=step.name,
        handler=step.handler or "humaneval_compress_ast",
        result=result,
        timestamp=datetime.now(tz=UTC).isoformat(),
    )
    return job
