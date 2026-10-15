"""Adds the second manifest layout: the same data files, packed into ~8 MB manifests.

Tags the current snapshot (one small manifest per commit) as small_manifests, runs
rewrite_manifests, and tags the result as compacted_manifests. Queries pick a layout with
VERSION AS OF '<tag>'. The tags also keep both snapshots from ever being expired.

Run from the project root:  bin/submit.sh setup/compact_manifests.py
Safe to run again: it skips whatever is already done.
"""

from pyspark.sql import SparkSession

from inspect_table import report

TABLE = "bench.metabench.events"


def refs(spark):
    spark.sql(f"REFRESH TABLE {TABLE}")
    return {r.name: r.snapshot_id for r in spark.sql(f"SELECT name, snapshot_id FROM {TABLE}.refs").collect()}


def operation(spark, snapshot_id):
    return spark.sql(f"SELECT operation FROM {TABLE}.snapshots WHERE snapshot_id = {snapshot_id}").first()[0]


spark = SparkSession.builder.appName("metabench-compact-manifests").getOrCreate()
current = refs(spark)

if "small_manifests" not in current:
    if operation(spark, current["main"]) != "append":
        raise SystemExit("expected the current snapshot to be populate.py's last append")
    spark.sql(f"ALTER TABLE {TABLE} CREATE TAG small_manifests AS OF VERSION {current['main']}")
    print(f"tagged snapshot {current['main']} as small_manifests")

if "compacted_manifests" not in current:
    if operation(spark, current["main"]) == "append":
        result = spark.sql("CALL bench.system.rewrite_manifests(table => 'metabench.events')").first()
        print(f"rewrote {result.rewritten_manifests_count} manifests into {result.added_manifests_count}")
        current = refs(spark)
    spark.sql(f"ALTER TABLE {TABLE} CREATE TAG compacted_manifests AS OF VERSION {current['main']}")
    print(f"tagged snapshot {current['main']} as compacted_manifests")

# main now points at the compacted snapshot; the small layout was reported by populate.py.
report(spark, "metabench.events", extrapolate=False)
spark.stop()
