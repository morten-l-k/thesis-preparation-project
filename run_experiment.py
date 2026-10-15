"""Runs the experiment: every query on every manifest layout, as job1 -> job2 pairs, 10 times.

One repetition is job1 then job2, back to back, each its own spark-submit (a new JVM) running
the same query on the same layout. For every job this script measures t_total, the wall-clock
time of the whole spark-submit, and adds it to the job's RESULT record. Repetitions run in a
fixed random order, so a slow stretch of network doesn't line up with one query or layout.

A repetition's two records are appended to results/runs.jsonl once both jobs have finished,
so an interrupted run can just be started again; finished repetitions are skipped.
Each job's Spark output goes to results/logs/ (results/logs-<name>/ for any other --out file).

Run from the project root, with .env filled in (standard library only, any python3):
  caffeinate -i python3 run_experiment.py                                  # full matrix, ~4 h
  python3 run_experiment.py --queries q1 --reps 1 --out results/trial.jsonl  # quick trial
caffeinate keeps the Mac awake; sleeping would stall the run and distort its timings.
"""

import argparse
import json
import os
import random
import subprocess
import time
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parent
QUERIES = ("q1", "q2", "q3", "q4", "q5")
LAYOUTS = ("small", "compacted")


def load_env(path):
    """Minimal .env reader (KEY=VALUE lines); real environment variables take precedence."""
    if not path.exists():
        raise SystemExit(f"Missing {path}. Copy .env.example to .env and fill it in.")
    for line in path.read_text().splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            key, value = line.split("=", 1)
            os.environ.setdefault(key.strip().removeprefix("export ").strip(), value.strip().strip("\"'"))


load_env(Path(os.environ.get("ENV_FILE", ROOT / ".env")))
REST_URI = os.environ.get("REST_URI") or f"http://localhost:{os.environ.get('REST_PORT', '8181')}"


def check_catalog():
    try:
        with urllib.request.urlopen(f"{REST_URI}/v1/config", timeout=5):
            pass
    except OSError as e:
        raise SystemExit(f"REST catalog not reachable at {REST_URI} ({e}). Is the docker compose stack up?")


def run_job(query, layout, job, rep, logs):
    log = logs / f"{query}-{layout}-run{rep:02d}-{job}.log"
    start = time.time()
    env = dict(os.environ, METABENCH_T0=repr(start))
    command = [str(ROOT / "bin" / "submit.sh"), "jobs/query_job.py",
               "--query", query, "--layout", layout, "--job", job, "--run", str(rep)]
    with log.open("w") as stderr:
        proc = subprocess.run(command, cwd=ROOT, env=env, stdout=subprocess.PIPE, stderr=stderr, text=True)
    t_total = (time.time() - start) * 1000
    with log.open("a") as out:
        out.write(proc.stdout)

    result = next((line[len("RESULT "):] for line in proc.stdout.splitlines() if line.startswith("RESULT ")), None)
    if proc.returncode != 0 or result is None:
        raise RuntimeError(f"{job} exited with code {proc.returncode}, see {log.relative_to(ROOT)}")
    record = json.loads(result)
    record["t_total_ms"] = t_total
    return record


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--queries", nargs="+", choices=QUERIES, default=list(QUERIES))
    parser.add_argument("--layouts", nargs="+", choices=LAYOUTS, default=list(LAYOUTS))
    parser.add_argument("--reps", type=int, default=10)
    parser.add_argument("--seed", type=int, default=1, help="fixes the random order of repetitions")
    parser.add_argument("--out", type=Path, default=ROOT / "results" / "runs.jsonl")
    args = parser.parse_args(argv)

    check_catalog()
    # The main experiment's logs stay in logs/; other output files (trials, bytes) get their own folder.
    logs = args.out.parent / ("logs" if args.out.name == "runs.jsonl" else f"logs-{args.out.stem}")
    logs.mkdir(parents=True, exist_ok=True)

    done = set()
    if args.out.exists():
        for line in args.out.read_text().splitlines():
            r = json.loads(line)
            done.add((r["query"], r["layout"], r["run"]))

    cells = [(q, l, rep) for q in args.queries for l in args.layouts for rep in range(1, args.reps + 1)]
    random.Random(args.seed).shuffle(cells)
    todo = [c for c in cells if c not in done]
    print(f"{len(cells) - len(todo)} of {len(cells)} repetitions already in {args.out.name}, {len(todo)} to run")

    failed = []
    for i, (query, layout, rep) in enumerate(todo, 1):
        try:
            pair = [run_job(query, layout, job, rep, logs) for job in ("job1", "job2")]
        except RuntimeError as e:
            failed.append((query, layout, rep))
            print(f"[{i}/{len(todo)}] {query} {layout:<9} run {rep:>2}: FAILED, {e}", flush=True)
            continue
        with args.out.open("a") as out:
            for record in pair:
                out.write(json.dumps(record) + "\n")
        summary = "   ".join(
            f"{r['job']} {r['t_total_ms'] / 1000:6.1f} s (fetch {r['t_fetch_ms'] / r['t_total_ms']:5.1%})" for r in pair
        )
        print(f"[{i}/{len(todo)}] {query} {layout:<9} run {rep:>2}: {summary}", flush=True)

    if failed:
        print(f"\n{len(failed)} repetitions failed and were not recorded; run the same command again to retry them")


if __name__ == "__main__":
    main()
