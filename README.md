# dr-bottleneck

LLM bottleneck experiment orchestration. This repo now stays intentionally
thin: provider calls, queue runtime, and code evaluation live in sibling
packages.

## Package Boundaries

| Package | Responsibility |
|---------|----------------|
| `dr-providers` | Typed OpenRouter requests/responses, API key loading, HTTP transport, retries |
| `dr-queues` | RabbitMQ/MongoDB pipeline runtime, manifests, worker lifecycle, run status |
| `dr-code` | HumanEval+ task models, attempt schemas, parse/test evaluation, proof reports |
| `dr-bottleneck` | Experiment configs, prompt rendering, LLM call audit logs, bottleneck run reports |

During the first experiment round these packages are installed from local
editable sibling checkouts through `tool.uv.sources`.

## Requirements

- Python 3.13+ with [uv](https://docs.astral.sh/uv/)
- RabbitMQ and MongoDB
- `OPENROUTER_API_KEY` for live LLM runs

```bash
uv sync
docker compose up -d
```

If `OPENROUTER_API_KEY` is not already exported, local live scripts source
`~/.envrc` before failing.

## Environment

| Variable | Default | Purpose |
|----------|---------|---------|
| `AMQP_URL` | `amqp://guest:guest@localhost:5672/` | RabbitMQ connection used by `dr-queues` |
| `MONGODB_URL` | `mongodb://localhost:27017/dr_queues` | `dr-queues` manifests, events, job state, workers |
| `BOTTLENECK_MONGODB_URL` | `mongodb://localhost:27017/dr_bottleneck` | Bottleneck reports and LLM call logs |
| `OPENROUTER_API_KEY` | required for LLM calls | OpenRouter API key used by `dr-providers` |

API keys are read by `dr-providers` at call time and are not stored in reports
or LLM call logs.

## Quick Start

Provider smoke across configured profiles:

```bash
scripts/run_openrouter_live.sh
```

Cheap two-step queue/provider smoke:

```bash
uv run python scripts/run_two_step_demo.py --repeats 1
```

HumanEval tiny run:

```bash
uv run python scripts/run_humaneval_demo.py --tiny
```

HumanEval runs are two phase:

1. `dr-bottleneck` runs encode -> decode through `dr-queues`.
2. Terminal encode/decode jobs become `dr-code` `AttemptRecord`s.
3. `dr-code` runs parse -> test as a linked eval run.
4. `dr-bottleneck.run_reports` stores the bottleneck report plus the linked
   `code_eval.run_id` and proof summary.

## Broader Bounded Live Check

Use this before trusting the refactor for the first experiment round:

```bash
uv run python scripts/run_humaneval_demo.py \
  --task-ids HumanEval/0,HumanEval/1,HumanEval/2 \
  --budgets 64,128 \
  --workers encode=4,decode=4 \
  --code-eval-workers parse=4,test=4
```

## Detached Workers

Detached workers are `dr-queues` workers using the bottleneck handler module:

```bash
uv run python scripts/run_two_step_demo.py --repeats 10 --no-wait
```

The command prints one `dr_queues.cli.stage_worker` command per stage. The
equivalent shape is:

```bash
uv run python -m dr_queues.cli.stage_worker \
  --run-id demo-abc123 \
  --stage add_five \
  --workers 5 \
  --handlers-module dr_bottleneck.handlers.queue
```

## Storage

| Database | Collection | Contents |
|----------|------------|----------|
| `dr_queues` | `run_manifests` | Runtime manifest and bottleneck workflow metadata |
| `dr_queues` | `pipeline_events` | Stage and terminal events |
| `dr_queues` | `job_states`, `job_attempts`, `workers` | Runtime state and worker lifecycle |
| `dr_bottleneck` | `run_reports` | Bottleneck run config, terminal jobs, overlap report, linked code eval summary |
| `dr_bottleneck` | `llm_calls` | Sanitized provider request/response audit records |

See [MONGODB_QUICKSTART.md](MONGODB_QUICKSTART.md) for query examples.

## Development

```bash
uv sync
scripts/pre-check.sh
```

Useful targeted checks:

```bash
uv run pytest
uv run ruff check
uv run ty check
```
