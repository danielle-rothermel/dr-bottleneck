from dr_providers.client import assistant_text, call_llm
from dr_providers.openrouter import MissingApiKeyError, build_completion_kwargs
from dr_providers.record import LOG_DIR, append_record, default_log_path

__all__ = [
    "LOG_DIR",
    "MissingApiKeyError",
    "append_record",
    "assistant_text",
    "build_completion_kwargs",
    "call_llm",
    "default_log_path",
]
