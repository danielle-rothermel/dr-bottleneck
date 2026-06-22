# MongoDB quickstart for dr-bottleneck

After a successful run, runtime state and experiment outputs are split by
package ownership.

| Database | Collection | Contents |
|----------|------------|----------|
| `dr_queues` | `run_manifests` | `dr-queues` manifest plus bottleneck workflow metadata |
| `dr_queues` | `pipeline_events` | Stage and terminal events |
| `dr_queues` | `job_states`, `job_attempts`, `workers` | Queue runtime state |
| `dr_bottleneck` | `run_reports` | Bottleneck report and linked code eval summary |
| `dr_bottleneck` | `llm_calls` | Sanitized `dr-providers` request/response records |

## Prerequisites

```bash
docker compose up -d
```

Each script prints `run_id=...`. Use the printed value in the queries below.

## Runtime State

Fetch the stored manifest and workflow metadata:

```bash
mongosh mongodb://localhost:27017/dr_queues \
  --eval 'db.run_manifests.findOne({run_id: "YOUR_RUN_ID"})'
```

Count pipeline events:

```bash
mongosh mongodb://localhost:27017/dr_queues \
  --eval 'db.pipeline_events.countDocuments({run_id: "YOUR_RUN_ID"})'
```

Count terminal jobs:

```bash
mongosh mongodb://localhost:27017/dr_queues \
  --eval 'db.pipeline_events.countDocuments({run_id: "YOUR_RUN_ID", event: "terminal"})'
```

Check worker records:

```bash
mongosh mongodb://localhost:27017/dr_queues \
  --eval 'db.workers.find({run_id: "YOUR_RUN_ID"}).pretty()'
```

## Bottleneck Report

Fetch the assembled report:

```bash
mongosh mongodb://localhost:27017/dr_bottleneck \
  --eval 'db.run_reports.findOne({run_id: "YOUR_RUN_ID"})'
```

Inspect only overlap and linked code eval summary:

```bash
mongosh mongodb://localhost:27017/dr_bottleneck \
  --eval 'db.run_reports.findOne({run_id: "YOUR_RUN_ID"}, {overlap_report: 1, code_eval: 1, _id: 0})'
```

Count jobs in a report:

```bash
mongosh mongodb://localhost:27017/dr_bottleneck \
  --eval 'db.run_reports.aggregate([{$match: {run_id: "YOUR_RUN_ID"}}, {$project: {job_count: {$size: "$jobs"}}}])'
```

## Linked dr-code Eval

HumanEval runs store the linked `dr-code` run id at
`run_reports.code_eval.run_id`. Query that run in `dr_queues`:

```bash
mongosh mongodb://localhost:27017/dr_queues \
  --eval 'db.pipeline_events.countDocuments({run_id: "YOUR_CODE_EVAL_RUN_ID", event: "terminal"})'
```

`dr-code` also exports proof artifacts under `exports/runs` by default.

## LLM Calls

Count provider calls for a bottleneck run:

```bash
mongosh mongodb://localhost:27017/dr_bottleneck \
  --eval 'db.llm_calls.countDocuments({run_id: "YOUR_RUN_ID"})'
```

Inspect one sanitized request/response:

```bash
mongosh mongodb://localhost:27017/dr_bottleneck \
  --eval 'db.llm_calls.findOne({run_id: "YOUR_RUN_ID"}, {request: 1, response: 1, latency_ms: 1, _id: 0})'
```

Filter by profile:

```bash
mongosh mongodb://localhost:27017/dr_bottleneck \
  --eval 'db.llm_calls.find({profile: "openrouter/google/gemini-2.5-flash/off/v1"}).limit(3).pretty()'
```

## Troubleshooting

**No runtime events**

- MongoDB or RabbitMQ was not running when the run started.
- You queried a placeholder instead of the printed `run_id`.
- You queried `dr_bottleneck`; runtime events live in `dr_queues`.

**No bottleneck report**

- The script exited before the post-run report step.
- For `--no-wait`, generation is seeded but report assembly is intentionally
  skipped.

**No linked code eval**

- The bottleneck generation phase did not complete.
- One or more terminal jobs had missing encode/decode output, so attempts could
  not be built.

**No LLM calls**

- No LLM stage ran, or provider calls failed before logging.
- Ad-hoc `query_provider.py` calls omit `run_id`; search by timestamp instead.
