import json
from pathlib import Path

import typer

from dr_queues.connection import open_session
from dr_queues.drain import dump_drain

app = typer.Typer(add_completion=False)


@app.command()
def main(
    out: Path = typer.Option(
        Path("exports/drain.jsonl"),
        "--out",
    ),
) -> None:
    session = open_session()
    try:
        events = dump_drain(session.channel)
    finally:
        session.close()

    out.parent.mkdir(parents=True, exist_ok=True)
    with out.open("w", encoding="utf-8") as handle:
        for event in events:
            handle.write(json.dumps(event))
            handle.write("\n")

    typer.echo(f"Exported {len(events)} drain events to {out}")


if __name__ == "__main__":
    app()
