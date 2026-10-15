"""One measured Spark job: runs one query once, in a fresh JVM, and reports where the time went.

  t_startup    process start until the SparkSession is ready
  t_catalog    spark.sql(): parse + analyze, which loads the table from the REST catalog
  t_query      collect(): Iceberg planning + execution
  t_manifests  Iceberg planning, from its ScanReport (part of t_query)
  t_fetch      time blocked on S3 reads of the manifest list and manifests (part of t_manifests)
  t_exec       t_query - t_manifests
  data read    time blocked on S3 reads of Parquet data files (part of t_exec), and their bytes

Run from the project root:
  bin/submit.sh jobs/query_job.py --query q2 --layout small

Prints a summary and one 'RESULT {json}' line. run_experiment.py (step 5) starts the jobs,
collects those lines and adds t_total, the wall-clock time of the whole spark-submit. It passes
its start time in METABENCH_T0; run by hand, t_startup counts from the JVM's start instead,
which misses spark-submit's own launcher (a few hundred ms).
"""

import argparse
import json
import os
import time

from pyspark.sql import SparkSession

TABLE = "bench.metabench.events"
LAYOUT_TAGS = {"small": "small_manifests", "compacted": "compacted_manifests"}
DAY_FILTER = "event_ts >= TIMESTAMP '{0}' AND event_ts < TIMESTAMP '{1}'"
QUERIES = {
    # Answered from manifest stats alone (aggregate pushdown): every manifest, no data files.
    "q1": "SELECT count(*) AS n FROM {table}",
    # One event. event_id isn't a partition column, so every manifest is read; stats leave one file.
    "q2": "SELECT * FROM {table} WHERE event_id = 5145000",
    # One day: partition pruning skips other days' manifests; 300 data files.
    "q3": "SELECT count(*) AS n, sum(amount_01) AS s, avg(amount_02) AS a FROM {table} WHERE "
    + DAY_FILTER.format("2025-06-15 00:00:00", "2025-06-16 00:00:00"),
    # One month: 30 days, 9,000 data files.
    "q4": "SELECT count(*) AS n, sum(amount_01) AS s, avg(amount_02) AS a FROM {table} WHERE "
    + DAY_FILTER.format("2025-06-01 00:00:00", "2025-07-01 00:00:00"),
    # q4's month again, but selected by event_id (days 151-180 x 15,000 ids per day). It isn't a
    # partition column, so every manifest is read; file stats still leave the same 9,000 files.
    "q5": "SELECT count(*) AS n, sum(amount_01) AS s, avg(amount_02) AS a FROM {table} "
    "WHERE event_id >= 2265000 AND event_id < 2715000",
}
TIME_UNITS_TO_MS = {"nanoseconds": 1e-6, "microseconds": 1e-3, "milliseconds": 1.0, "seconds": 1e3}


def metric(scan_report, name):
    value = scan_report["metrics"].get(name, {})
    if "total-duration" in value:
        return value["total-duration"] * TIME_UNITS_TO_MS[value["time-unit"]]
    return value.get("value", 0)


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--query", choices=sorted(QUERIES), required=True)
    parser.add_argument("--layout", choices=sorted(LAYOUT_TAGS), required=True)
    parser.add_argument("--job", default="manual", help="label, e.g. job1 or job2")
    parser.add_argument("--run", type=int, default=0, help="repetition number")
    args = parser.parse_args()

    spark = SparkSession.builder.appName(f"metabench-{args.query}-{args.layout}-{args.job}").getOrCreate()
    session_ready = time.time()
    if "METABENCH_T0" in os.environ:
        t0, t0_source = float(os.environ["METABENCH_T0"]), "spark-submit"
    else:
        t0 = spark._jvm.java.lang.management.ManagementFactory.getRuntimeMXBean().getStartTime() / 1000
        t0_source = "jvm"

    probes = spark._jvm.thesis.metabench
    probes.FetchStats.reset()
    probes.CollectingScanReporter.drainJson()

    sql = QUERIES[args.query].format(table=f"{TABLE} VERSION AS OF '{LAYOUT_TAGS[args.layout]}'")
    before_sql = time.perf_counter()
    df = spark.sql(sql)
    before_collect = time.perf_counter()
    rows = df.collect()
    after_collect = time.perf_counter()

    fetch = json.loads(probes.FetchStats.snapshotJson())
    reports = json.loads(probes.CollectingScanReporter.drainJson())
    plan = df._jdf.queryExecution().executedPlan().toString()

    t_query = (after_collect - before_collect) * 1000
    t_manifests = sum(metric(r, "total-planning-duration") for r in reports)
    record = {
        "query": args.query,
        "layout": args.layout,
        "job": args.job,
        "run": args.run,
        "t0": t0,
        "t0_source": t0_source,
        "t_startup_ms": (session_ready - t0) * 1000,
        "t_catalog_ms": (before_collect - before_sql) * 1000,
        "t_query_ms": t_query,
        "t_manifests_ms": t_manifests,
        "t_fetch_ms": fetch["t_fetch_ms"],
        "t_exec_ms": t_query - t_manifests,
        "t_data_read_ms": fetch["data_read_ms"],
        "scan_reports": len(reports),
        "manifests_total": sum(metric(r, "total-data-manifests") for r in reports),
        "manifests_scanned": sum(metric(r, "scanned-data-manifests") for r in reports),
        "data_files_matched": sum(metric(r, "result-data-files") for r in reports),
        "reads_data_files": "BatchScan" in plan,
        "fetch": fetch["kinds"],
        "result": [r.asDict() for r in rows] if args.query != "q2" else len(rows),
    }

    kinds = fetch["kinds"]
    fetched = kinds["manifest_list"]["bytes"] + kinds["manifest"]["bytes"]
    print(f"\n{args.query} on {LAYOUT_TAGS[args.layout]} ({args.job}, run {args.run}): {record['result']}")
    print(f"  t_startup    {record['t_startup_ms']:>9,.0f} ms   (from {t0_source} start)")
    print(f"  t_catalog    {record['t_catalog_ms']:>9,.0f} ms")
    print(f"  t_query      {t_query:>9,.0f} ms")
    print(f"  t_manifests  {t_manifests:>9,.0f} ms   ({record['manifests_scanned']:,} of "
          f"{record['manifests_total']:,} manifests scanned, {record['data_files_matched']:,} data files matched)")
    print(f"  t_fetch      {fetch['t_fetch_ms']:>9,.0f} ms   ({kinds['manifest_list']['files']} manifest list + "
          f"{kinds['manifest']['files']:,} manifests, {fetched / 1024 / 1024:,.1f} MB)")
    print(f"  t_exec       {record['t_exec_ms']:>9,.0f} ms   "
          f"({'reads data files' if record['reads_data_files'] else 'no data files read'})")
    print(f"    data read  {fetch['data_read_ms']:>9,.0f} ms   ({kinds['data']['files']:,} data files, "
          f"{kinds['data']['bytes'] / 1024 / 1024:,.1f} MB)")
    print("RESULT " + json.dumps(record, default=str), flush=True)  # str: sums/averages are Decimals
    spark.stop()


if __name__ == "__main__":
    main()
