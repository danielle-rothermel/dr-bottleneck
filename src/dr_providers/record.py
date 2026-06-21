import json
from datetime import UTC, datetime
from pathlib import Path

LOG_DIR = Path("logs")


def default_log_path(*, when: datetime | None = None) -> Path:
    moment = when or datetime.now(tz=UTC)
    date_str = moment.date().isoformat()
    return LOG_DIR / f"calls-{date_str}.jsonl"


def append_record(path: Path, record: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(record))
        handle.write("\n")
