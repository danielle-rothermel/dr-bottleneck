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

## Workflow runs (canonical pattern)

Each workflow run writes a manifest at `.runs/{run_id}/manifest.json` with
queue names, workflow paths, and per-stage worker defaults.

**Default (in-process):** one kickoff command starts all stage worker pools in
the same process, seeds jobs, waits for completion, and exports results.

**Detached workers:** pass `--start-workers` to spawn one
`scripts/run_stage_workers.py` process per stage. Use `--no-wait` to seed jobs
and exit without waiting (prints worker start commands).

**Resize a stage while running:**

```bash
uv run python scripts/run_stage_workers.py \
  --run-id demo-abc123 \
  --stage decode \
  --workers 5 \
  --replace
```

**Per-stage worker counts** use comma-separated `name=count` pairs:

```bash
--workers random_number=20,add_five=10
--workers encode=8,decode=8,evaluate=32
```

The global durable queue `dr.drain` holds stage and terminal events. Normal
runs peek it without purging. To export and remove messages:

```bash
uv run python scripts/drain_dump.py --out exports/drain.jsonl
```

## Two-step workflow demo

Runs a pipelined two-stage LLM workflow: random number, then add 5. Uses 4
paired model lanes × 10 repeats (40 jobs).

```bash
export OPENROUTER_API_KEY=...
uv run python scripts/run_two_step_demo.py
```

Options:

```bash
uv run python scripts/run_two_step_demo.py \
  --repeats 10 \
  --workers random_number=20,add_five=20 \
  --workflow configs/workflows/two_step_random.yaml \
  --dump-out exports/demo-run.jsonl

uv run python scripts/run_two_step_demo.py --start-workers --no-wait

uv run python scripts/run_stage_workers.py \
  --run-id demo-abc123 \
  --stage add_five \
  --workers 5 \
  --replace
```

Kickoff prints the manifest path (`.runs/{run_id}/manifest.json`). The demo
writes a JSONL run report to `exports/run-{run_id}.jsonl` (or `--dump-out`).

| `record_type` | Contents |
|---------------|----------|
| `config` | Workflow path, lanes, steps, repeats, workers_by_stage |
| `job` | Per-job stage inputs/responses and `final_result` |
| `overlap_report` | Pipeline overlap analysis (last line) |

Workflow configs live in `configs/workflows/`. Lanes specify per-step model
profiles; steps define prompts and templates (`{prev_output}` for chaining).

## HumanEval encode/decode/eval demo

Runs HumanEval+ tasks through encode → decode → evaluate (zstd compression +
AST parse). Jobs carry `task_id`, `prompt`, `canonical_solution`, and
`entry_point` through the pipeline (`test` is omitted; rejoin via dataset later).

**Smoke test (2 jobs, in-process):**

```bash
export OPENROUTER_API_KEY=...
uv run python scripts/run_humaneval_demo.py --tiny
```

**Preview prompts (no LLM calls):**

```bash
uv run python scripts/preview_humaneval_prompts.py -n 3 --seed 42
```

**Full sweep:** 164 tasks × 3 models × 6 budgets × 1 repeat = 2,952 jobs.
Full runs spawn detached stage workers by default.

```bash
uv run python scripts/run_humaneval_demo.py \
  --workers encode=8,decode=8,evaluate=32 \
  --budgets 32,64,128,256,512,1024
```

Outputs:

- `exports/humaneval-{run_id}.jsonl` — full run report
- `exports/metrics-{run_id}.jsonl` — flat rows for plotting

Metrics row schema:

| field | meaning |
|-------|---------|
| `model` | decode model |
| `budget` | character budget metadata |
| `sample_id` | HumanEval task id |
| `encoded_len_raw` | UTF-8 byte length of encode output |
| `encoded_len_compressed` | zstd-compressed size |
| `pass` | 1 if decode output AST-parses, else 0 |

## Single LLM query

```bash
uv run python scripts/query_provider.py \
  --profile openrouter/google/gemini-2.5-flash/off/v1 \
  --message "Hello"
```

## Package layout

- `src/dr_providers/` — LiteLLM / OpenRouter client and JSONL call logging
- `src/dr_queues/` — RabbitMQ workflow pipeline (see module map below)

### `dr_queues` module map

Layers are ordered so only `connection.py` imports pika; higher layers
compose the primitives below them.

| Module | Role |
|--------|------|
| `connection.py` | AMQP connection, channel session, publish, delivery tags |
| `queues.py` | Per-stage durable queue pairs (`build_stage_queues`) |
| `drain.py` | Global `dr.drain` event queue (peek, dump, stage/terminal events) |
| `job.py` | `JobEnvelope`, `StepExecution`, `ProcessStepResult`, `seed_jobs` |
| `workflow.py` | YAML workflow config, LLM/process stage handlers |
| `humaneval_data.py` | HumanEval+ task loading and experiment job expansion |
| `workers.py` | `WorkerPool` — parallel stage consumers |
| `tap.py` | `TerminalTap` — completion counter on final stage queue |
| `runner.py` | Manifest setup, in-process orchestration, detached worker spawn |
| `manifest.py` | `.runs/{run_id}/manifest.json` read/write |
| `report.py` / `analyze.py` | Run report assembly and pipeline overlap analysis |
| `metrics_report.py` | HumanEval metrics JSONL export |

Stage queues chain by passing the previous stage's **full completed queue
name** as the next stage's input (`build_stage_queues(pending=...)`).

Open a channel via `ChannelSession.open_session()` (not a top-level
`open_session` export).

### Public API (import from `dr_queues`)

- `setup_run_queues` / `run_workflow_in_process` — manifest-backed pipeline
- `seed_jobs` / `seed_manifest_jobs` — enqueue `JobEnvelope` payloads
- `WorkerPool` — parallel stage workers; `TerminalTap` — completion wait
- `Workflow.from_yaml` — load workflow + LLM/process stage handlers
- `build_run_report` / `write_run_report_jsonl` — assemble exportable reports
- `build_metrics_rows` / `write_metrics_jsonl` — HumanEval metrics export
- `peek_drain` / `DRAIN_QUEUE` — inspect global drain without purging

Scripts:

- `scripts/run_two_step_demo.py` — two-stage LLM pipeline demo
- `scripts/run_humaneval_demo.py` — HumanEval encode/decode/eval sweep
- `scripts/run_stage_workers.py` — detached single-stage worker pool
- `scripts/drain_dump.py` — export and purge `dr.drain`
