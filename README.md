# dr-bottleneck

LLM bottleneck tooling: OpenRouter providers via LiteLLM and RabbitMQ workflow queues.

## Requirements

- Python 3.13+ with [uv](https://docs.astral.sh/uv/)
- RabbitMQ (local via Docker Compose or existing broker)
- `OPENROUTER_API_KEY` for LLM demo runs

## Environment

| Variable | Default | Purpose |
|----------|---------|---------|
| `AMQP_URL` | `amqp://guest:guest@localhost:5672/` | RabbitMQ connection |
| `OPENROUTER_API_KEY` | (required for LLM calls) | OpenRouter API key |

API keys are read from the environment at call time only. They are **not**
stored in drain payloads, run reports, or JSONL logs.

## Local RabbitMQ

```bash
docker compose up -d
```

Management UI: http://localhost:15672 (guest/guest)

## Two-step workflow demo

Runs a pipelined two-stage LLM workflow: random number, then add 5. Uses 4
paired model lanes × 10 repeats (40 jobs), 20 workers per stage.

```bash
export OPENROUTER_API_KEY=...
uv run python scripts/run_two_step_demo.py
```

Options:

```bash
uv run python scripts/run_two_step_demo.py \
  --repeats 10 \
  --workers 20 \
  --workflow configs/workflows/two_step_random.yaml \
  --dump-out exports/demo-run.jsonl
```

The demo prints a pipeline overlap report and writes a JSONL run report to
`exports/run-{run_id}.jsonl` (or `--dump-out`). Each line is one JSON object:

| `record_type` | Contents |
|---------------|----------|
| `config` | Workflow path, lanes, steps, repeats, workers |
| `job` | Per-job stage inputs/responses and `final_result` |
| `overlap_report` | Pipeline overlap analysis (last line) |

Query examples:

```bash
head -n 1 exports/run-demo-abc123.jsonl | jq          # config
grep '"record_type":"job"' exports/run-demo-abc123.jsonl | tail -n 1 | jq
tail -n 1 exports/run-demo-abc123.jsonl | jq          # overlap_report
```

Each job record includes per-stage `prompt`, `messages`, `request`,
`response`, `assistant_text`, and `latency_ms`. The `request` object omits
`api_key`.

Workflow configs live in `configs/workflows/`. Lanes specify per-step model
profiles; steps define prompts and templates (`{prev_output}` for chaining).

## Drain export (escape hatch)

The global durable queue `dr.drain` holds stage and terminal events during
runs. Normal demo runs peek it without purging.

If the drain queue grows too large, export and remove messages:

```bash
uv run python scripts/drain_dump.py --out exports/drain.jsonl
```

This consumes messages from the drain queue. Normal demo runs do not require
this step.

## Single LLM query

```bash
uv run python scripts/query_provider.py \
  --profile openrouter/google/gemini-2.5-flash/off/v1 \
  --message "Hello"
```

## Package layout

- `src/dr_providers/` — LiteLLM / OpenRouter client and JSONL call logging
- `src/dr_queues/` — RabbitMQ workflow primitives (queues, workers, drain,
  workflow config, run reports)

### `dr_queues` API (prototype)

- `build_stage_queues` — create or inject pending/completed queues
- `WorkerPool` — parallel stage workers
- `seed_jobs` — publish initial jobs to pending
- `TerminalTap` — drain-only consumer on final completed queue
- `Workflow.from_yaml` — load workflow + LLM stage handlers
- `build_run_report` / `write_run_report_jsonl` — assemble exportable reports
