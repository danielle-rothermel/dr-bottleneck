from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

from dr_providers.record import LOG_DIR, append_record, default_log_path


def test_default_log_path_includes_date() -> None:
    path = default_log_path(when=datetime(2026, 6, 21, 12, 0, tzinfo=UTC))
    assert path == LOG_DIR / "calls-2026-06-21.jsonl"


def test_append_record_creates_dir_and_writes_jsonl(tmp_path: Path) -> None:
    log_path = tmp_path / "nested" / "calls-2026-06-21.jsonl"
    record = {
        "request": {"model": "openrouter/test"},
        "response": {"ok": True},
    }

    append_record(log_path, record)

    assert log_path.exists()
    lines = log_path.read_text(encoding="utf-8").splitlines()
    assert len(lines) == 1
    assert json.loads(lines[0]) == record
