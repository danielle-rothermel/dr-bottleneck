# dr-bottleneck

LLM bottleneck tooling: OpenRouter providers via LiteLLM and RabbitMQ workflow
queues, built on [dr-queues](https://pypi.org/project/dr-queues/).

## Requirements

- Python 3.13+ with [uv](https://docs.astral.sh/uv/)
- RabbitMQ and MongoDB (local via Docker Compose or existing infrastructure)
- `OPENROUTER_API_KEY` for LLM demo runs

## Environment

| Variable | Default | Purpose |
|----------|---------|---------|
| `AMQP_URL` | `amqp://guest:guest@localhost:5672/` | RabbitMQ connection |
| `MONGODB_URL` | `mongodb://localhost:27017/dr_queues` | Pipeline event store |
| `BOTTLENECK_MONGODB_URL` | `mongodb://localhost:27017/dr_bottleneck` | Reports, metrics, LLM calls |
| `OPENROUTER_API_KEY` | (required for LLM calls) | OpenRouter API key |

API keys are read from the environment at call time only. They are **not**
stored in pipeline events, run reports, or MongoDB logs.

## Local services

```bash
docker compose up -d
```

- RabbitMQ management UI: http://localhost:15672 (guest/guest)
- MongoDB: `mongodb://localhost:27017`

See [MONGODB_QUICKSTART.md](MONGODB_QUICKSTART.md) for querying results after a
successful run.

## Workflow runs (canonical pattern)

Each workflow run writes a manifest at `.runs/{run_id}/manifest.json` with queue
names, workflow paths, and per-stage worker defaults. Pipeline events go to
MongoDB (`dr_queues.pipeline_events`); assembled reports and metrics go to
`dr_bottleneck`.

**Default (in-process):** one kickoff command starts all stage worker pools in
the same process, seeds jobs, waits for completion, and persists results to
MongoDB.

**Detached workers:** pass `--start-workers` to spawn one
`dr-bottleneck-stage-worker` process per stage. Use `--no-wait` to seed jobs
and exit without waiting (prints worker start commands).

**Resize a stage while running:**

```bash
uv run dr-bottleneck-stage-worker \
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
  --workflow configs/workflows/two_step_random.yaml

uv run python scripts/run_two_step_demo.py --start-workers --no-wait
```

Kickoff prints `run_id=...` and stores the run report in
`dr_bottleneck.run_reports`. The demo exits with code 1 if the pipeline overlap
check fails.

## HumanEval encode/decode/eval demo

Runs HumanEval+ tasks through encode → decode → evaluate (zstd compression +
AST parse).

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

Outputs (MongoDB):

- `dr_bottleneck.run_reports` — full run report
- `dr_bottleneck.run_metrics` — flat rows + pass-rate summary

## Single LLM query

```bash
uv run python scripts/query_provider.py \
  --profile openrouter/google/gemini-2.5-flash/off/v1 \
  --message "Hello"
```

Calls are stored in `dr_bottleneck.llm_calls`.

## Package layout

- `src/dr_bottleneck/` — domain workflow engine, HumanEval experiments,
  analysis, MongoDB storage, runtime orchestration, and LLM client (uses
  dr-queues for AMQP + pipeline workers)
- `src/dr_bottleneck/llm/` — LiteLLM / OpenRouter client and MongoDB call
  logging

### dr-queues dependency

dr-bottleneck imports from the published `dr-queues` package:

- `WorkerPool`, `TerminalTap`, `JobEnvelope`, `seed_jobs`
- `MongoEventSink`, `PipelineEvent`, `EventKind`
- `build_stage_queues`, `parse_workers_arg`, manifest path helpers

Domain code stays in `dr_bottleneck` — YAML workflows, LLM/process handlers,
HumanEval job expansion, reports, and metrics.

### Public API (import from `dr_bottleneck`)

- `setup_run_queues` / `run_workflow_in_process` — manifest-backed pipeline
- `seed_manifest_jobs` — enqueue jobs on the first stage queue
- `Workflow.from_yaml` — load workflow + LLM/process stage handlers
- `build_run_report` / `persist_run_report` — assemble and store reports
- `build_metrics_rows` / `persist_run_metrics` — HumanEval metrics
- `peek_run_events` — read pipeline events from MongoDB
- HumanEval helpers: `load_humanevalplus`, `expand_experiment_jobs`, etc.

Scripts:

- `scripts/run_two_step_demo.py` — two-stage LLM pipeline demo
- `scripts/run_humaneval_demo.py` — HumanEval encode/decode/eval sweep
- `scripts/run_stage_workers.py` — detached single-stage worker pool
- `dr-bottleneck-stage-worker` — console entry point for detached workers

## Development

```bash
uv sync
docker compose up -d
scripts/pre-check.sh
```
