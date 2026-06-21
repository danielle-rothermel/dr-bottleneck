from __future__ import annotations

from dr_bottleneck.storage.inspect import format_mongo_inspect_hints


def test_format_mongo_inspect_hints_includes_run_id() -> None:
    run_id = "humaneval-abc123"
    hints = format_mongo_inspect_hints(run_id, include_metrics=True)
    joined = "\n".join(hints)
    assert run_id in joined
    assert "db.run_reports.findOne" in joined
    assert "db.run_metrics.findOne" in joined
    assert "db.pipeline_events.countDocuments" in joined


def test_format_mongo_inspect_hints_two_step() -> None:
    hints = format_mongo_inspect_hints("demo-abc123")
    joined = "\n".join(hints)
    assert "db.run_reports.findOne" in joined
    assert "db.run_metrics.findOne" not in joined
