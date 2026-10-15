# Thesis preparation project
This repo contains a setup for testing runtime of different phases, when running two consecutive Spark jobs. The test is intended to establish, how dominant the gap is for fetching manifest files from remote S3 storage. 

## test-spark (metabench)

How much of a whole Spark job, from `spark-submit` to exit, goes to fetching Iceberg
manifests from S3? Every job is a new JVM, so nothing job 1 fetched helps job 2.

Each job runs one query, once, cold, and records:

| Field | Meaning | Source |
|---|---|---|
| `t_total` | `spark-submit` start to process exit | orchestrator |
| `t_startup` | until the SparkSession is ready | job |
| `t_catalog` | loading the table from the REST catalog | job |
| `t_manifests` | Iceberg planning: fetch + decode + filter manifest list and manifests | `CollectingScanReporter` |
| `t_fetch` | wall-clock time blocked on S3 reads of manifest list and manifests | `TimingFileIO` |
| `t_exec` | the rest of the query | job |

## Versions

Spark 4.0 (pyspark 4.0.4, Scala 2.13) on Java 21 · Iceberg 1.10.1 (Spark runtime and REST
catalog image) · REST catalog backed by SQLite, warehouse at `s3://<bucket>/<S3_WAREHOUSE_PREFIX>/`
(default prefix `test-spark`).

## Prerequisites

Java 21, Python 3.9+, Docker (with compose), and an **S3 bucket** you can read and write, plus
AWS credentials for it. The experiment writes ~10 GB there.

## Setup

All commands run from the project root. Every setting comes from a `.env` file in the root
that you create yourself:

```bash
cp .env.example .env     # then edit .env
```

| Variable | Required | Meaning |
|---|---|---|
| `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY` | yes | credentials with access to the bucket |
| `AWS_REGION` | yes | region of the bucket |
| `S3_BUCKET_NAME` | yes | existing bucket; tables go to `s3://<bucket>/<S3_WAREHOUSE_PREFIX>/` |
| `S3_WAREHOUSE_PREFIX` | no | prefix inside the bucket, default `test-spark` |
| `REST_PORT` | no | host port of the REST catalog, default `8181` |
| `REST_URI` | no | full catalog URL, overrides `REST_PORT` (e.g. catalog on another host) |
| `SPARK_DRIVER_MEMORY` | no | default `4g` |
| `COMPOSE_PROJECT_NAME` | no | docker compose project / container name prefix, default `metabench` |

`.env` is git-ignored and is read by `bin/submit.sh`, `run_experiment.py` and `docker compose`.
Set `ENV_FILE=/path/to/other.env` to use a file elsewhere (for `submit.sh` and
`run_experiment.py`; `docker compose` needs `--env-file` for that).

```bash
python3 -m venv .venv && .venv/bin/pip install -r requirements.txt   # bin/submit.sh uses .venv/
source .venv/bin/activate                 # Optional
bin/download_jars.sh                      # Iceberg runtime + AWS bundle into jars/
(cd probes && ./gradlew jar)              # metabench-probes.jar into jars/
docker compose up -d                      # REST catalog on localhost:$REST_PORT (8181)
bin/submit.sh setup/check_access.py       # end-to-end check, cleans up after itself
```

## The table

`bench.metabench.events`: 100 columns (all with min/max stats), partitioned by
`days(event_ts)`, one commit per day starting 2025-01-01. Each commit writes
`files-per-commit` Parquet files with `rows-per-file` rows each; `event_id` is contiguous
per file. Manifest merging is off, so each commit adds exactly one manifest.

```bash
bin/submit.sh --master "local[32]" setup/populate.py --commits 5     # pilot: first 5 days
bin/submit.sh --master "local[32]" setup/populate.py --commits 686   # full: ~200 MB of manifests
bin/submit.sh setup/compact_manifests.py                             # adds the compacted layout
bin/submit.sh setup/inspect_table.py                                 # sizes again, any time
```

`populate.py` continues from the commits already in the table, so the full fill reuses the
pilot's days. `--recreate` drops the table and its files and starts over, e.g. with other
`--files-per-commit` / `--rows-per-file` values. Each run saves the table's sizes to `results/`.

As built: 686 commits, 205,800 data files, 10.29M rows, 9.4 GB. Two tagged snapshots hold the
same files in different manifest layouts:

| Tag | Manifests |
|---|---|
| `small_manifests` | one per commit (686), as written |
| `compacted_manifests` | ~8 MB each, after `rewrite_manifests` |

## The queries

`jobs/query_job.py --query q1..q4 --layout small|compacted` runs one query against one layout:

| Query | What | Manifests read | Data files read |
|---|---|---|---|
| q1 | `count(*)` | all | none (answered from manifest stats) |
| q2 | one `event_id` | all | 1 |
| q3 | one day, `count`/`sum`/`avg` | that day's | 300 |
| q4 | one month, same aggregates | that month's | 9,000 |
| q5 | q4's month selected by `event_id` range | all | 9,000 (same as q4) |

## Running the experiment

```bash
python3 run_experiment.py --queries q1 --reps 1 --out results/trial.jsonl  # trial, ~1 min
caffeinate -i python3 run_experiment.py                                    # full: 160 jobs, ~4 h
```

Each repetition runs job1 and then job2 (two `spark-submit`s, so two fresh JVMs) and appends
both records, with `t_total` added, to `results/runs.jsonl`. Repetitions run in a fixed random
order. Interrupt it at any time and rerun the same command to continue. q4 dominates the
runtime at about 4.7 minutes per job.

## Analysis

```bash
python3 analysis/analyze.py      # any Python with matplotlib; the system python3 has it
```

Splits each job's `t_total` into startup, table load, manifest fetch, manifest decode & filter,
execution and shutdown (they add up exactly). Prints mean and min-max per query and layout,
and writes `results/summary.csv` (per query, layout and job) and `results/time_split.png/.pdf`.

Manifest bytes vs data bytes: since the 15:11 build of `metabench-probes.jar`, `TimingFileIO`
also counts Parquet data-file reads. The main runs predate that, so the bytes come from one
extra repetition (byte counts are deterministic, one is enough):

```bash
caffeinate -i python3 run_experiment.py --reps 1 --out results/bytes.jsonl   # ~25 min
python3 analysis/analyze.py                                                   # adds the bytes table
```

## Layout

```
bin/submit.sh          spark-submit with conf/, jars/ and .env
bin/download_jars.sh   pinned Iceberg jars, SHA-1 checked
conf/                  Spark and log4j settings
probes/                Gradle project: TimingFileIO, FetchStats, CollectingScanReporter
setup/                 check_access.py, populate.py, compact_manifests.py, inspect_table.py
jobs/query_job.py      the measured job: one query, once, cold
run_experiment.py      runs all job1 -> job2 repetitions, writes results/runs.jsonl
analysis/analyze.py    table, summary.csv and the time-split figure
docker-compose.yaml    REST catalog
.env.example          template for the .env you create (not committed: .env)
```
