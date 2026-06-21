# MongoDB quickstart for dr-bottleneck

After a successful demo run, pipeline telemetry and experiment outputs live in
two MongoDB databases:

| Database | Collection | Contents |
|----------|------------|----------|
| `dr_queues` | `pipeline_events` | Append-only stage/terminal events (via dr-queues) |
| `dr_bottleneck` | `run_reports` | Assembled run config, jobs, overlap report |
| `dr_bottleneck` | `run_metrics` | HumanEval flat metric rows + summary |
| `dr_bottleneck` | `llm_calls` | Individual LLM request/response records |

## Prerequisites

```bash
docker compose up -d
```

Environment variables (defaults shown):

| Variable | Default | Purpose |
|----------|---------|---------|
| `MONGODB_URL` | `mongodb://localhost:27017/dr_queues` | Pipeline events (`MongoEventSink`) |
| `BOTTLENECK_MONGODB_URL` | `mongodb://localhost:27017/dr_bottleneck` | Reports, metrics, LLM calls |
| `AMQP_URL` | `amqp://guest:guest@localhost:5672/` | RabbitMQ |

Each demo prints `run_id=...` at startup. Use that value in the queries below
— not the literal placeholder `YOUR_RUN_ID`.

## 1. Pipeline events (`dr_queues.pipeline_events`)

Count events for one run:

```bash
mongosh mongodb://localhost:27017/dr_queues \
  --eval 'db.pipeline_events.countDocuments({run_id: "YOUR_RUN_ID"})'
```

List all run IDs that have events:

```bash
mongosh mongodb://localhost:27017/dr_queues \
  --eval 'db.pipeline_events.distinct("run_id")'
```

Preview the first few events (sorted by timestamp):

```bash
mongosh mongodb://localhost:27017/dr_queues \
  --eval 'db.pipeline_events.find({run_id: "YOUR_RUN_ID"}).sort({timestamp: 1}).limit(5).pretty()'
```

Count terminal events (completed jobs):

```bash
mongosh mongodb://localhost:27017/dr_queues \
  --eval 'db.pipeline_events.countDocuments({run_id: "YOUR_RUN_ID", event: "terminal"})'
```

Filter by stage:

```bash
mongosh mongodb://localhost:27017/dr_queues \
  --eval 'db.pipeline_events.find({run_id: "YOUR_RUN_ID", stage: "encode"}).limit(3).pretty()'
```

Event document shape:

```json
{
  "event_id": "<uuid>",
  "run_id": "<str>",
  "job_id": "<str>",
  "lane": "<str>",
  "stage": "<str>",
  "event": "stage_started" | "stage_output" | "terminal",
  "timestamp": "<ISO8601>",
  "payload": { ... }
}
```

Terminal event payloads contain the full job envelope (domain fields in
`payload` and `step_records`).

## 2. Run reports (`dr_bottleneck.run_reports`)

Fetch the assembled report for a run:

```bash
mongosh mongodb://localhost:27017/dr_bottleneck \
  --eval 'db.run_reports.findOne({run_id: "YOUR_RUN_ID"})'
```

List all runs with stored reports:

```bash
mongosh mongodb://localhost:27017/dr_bottleneck \
  --eval 'db.run_reports.distinct("run_id")'
```

Inspect overlap analysis only:

```bash
mongosh mongodb://localhost:27017/dr_bottleneck \
  --eval 'db.run_reports.findOne({run_id: "YOUR_RUN_ID"}, {overlap_report: 1, _id: 0})'
```

Count jobs in a report:

```bash
mongosh mongodb://localhost:27017/dr_bottleneck \
  --eval 'db.run_reports.aggregate([{$match: {run_id: "YOUR_RUN_ID"}}, {$project: {job_count: {$size: "$jobs"}}}])'
```

## 3. Metrics (`dr_bottleneck.run_metrics`)

HumanEval demo runs store flat rows plus a summary:

```bash
mongosh mongodb://localhost:27017/dr_bottleneck \
  --eval 'db.run_metrics.findOne({run_id: "YOUR_RUN_ID"}, {summary: 1, _id: 0})'
```

Preview metric rows:

```bash
mongosh mongodb://localhost:27017/dr_bottleneck \
  --eval 'db.run_metrics.findOne({run_id: "YOUR_RUN_ID"}, {rows: {$slice: 5}})'
```

Pass rate by model (from stored summary):

```bash
mongosh mongodb://localhost:27017/dr_bottleneck \
  --eval 'db.run_metrics.findOne({run_id: "YOUR_RUN_ID"}, {"summary.by_model": 1, _id: 0})'
```

Metrics row fields:

| field | meaning |
|-------|---------|
| `model` | decode model |
| `budget` | character budget metadata |
| `sample_id` | HumanEval task id |
| `encoded_len_raw` | UTF-8 byte length of encode output |
| `encoded_len_compressed` | zstd-compressed size |
| `pass` | 1 if decode output AST-parses, else 0 |

## 4. LLM calls (`dr_bottleneck.llm_calls`)

Count calls for a pipeline run:

```bash
mongosh mongodb://localhost:27017/dr_bottleneck \
  --eval 'db.llm_calls.countDocuments({run_id: "YOUR_RUN_ID"})'
```

Preview recent calls (ad-hoc queries via `query_provider.py` have no `run_id`):

```bash
mongosh mongodb://localhost:27017/dr_bottleneck \
  --eval 'db.llm_calls.find().sort({timestamp: -1}).limit(3).pretty()'
```

Inspect one call's request/response:

```bash
mongosh mongodb://localhost:27017/dr_bottleneck \
  --eval 'db.llm_calls.findOne({run_id: "YOUR_RUN_ID"}, {request: 1, response: 1, latency_ms: 1, _id: 0})'
```

Filter by profile:

```bash
mongosh mongodb://localhost:27017/dr_bottleneck \
  --eval 'db.llm_calls.find({profile: "openrouter/google/gemini-2.5-flash/off/v1"}).limit(3).pretty()'
```

## 5. Cross-collection workflow

Given a `run_id` printed by a demo:

1. **Verify pipeline completed** — terminal count in `dr_queues.pipeline_events`
   should match `expected_jobs` from the manifest at
   `.runs/{run_id}/manifest.json`.
2. **Check assembled report** — `dr_bottleneck.run_reports` should exist with
   the same `run_id`.
3. **HumanEval only** — `dr_bottleneck.run_metrics` should have rows and a
   summary with pass rates.
4. **Trace one job** — find a `job_id` in terminal events, then filter LLM
   calls:

```bash
mongosh mongodb://localhost:27017/dr_bottleneck \
  --eval 'db.llm_calls.find({run_id: "YOUR_RUN_ID", job_id: "JOB_ID"}).pretty()'
```

## Troubleshooting

**Empty `pipeline_events`**

- MongoDB was not running when the demo started (`docker compose up -d`).
- Wrong database: pipeline events go to `dr_queues`, not `dr_bottleneck`.
- You used a placeholder instead of the printed `run_id`.

**Empty `run_reports` or `run_metrics`**

- The demo exited before the post-run persistence step (timeout, overlap
  failure on two-step demo, or `--no-wait`).
- Check `BOTTLENECK_MONGODB_URL` points at the database you are querying.

**Empty `llm_calls`**

- No LLM steps ran (evaluate-only path) or calls failed before logging.
- Ad-hoc `query_provider.py` calls omit `run_id`; search by `timestamp` instead.

**Manifest vs Mongo mismatch**

- Manifest lives on disk at `.runs/{run_id}/manifest.json` (queue names, worker
  defaults). Mongo holds telemetry and assembled outputs — both use the same
  `run_id`.
