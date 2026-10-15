"""Step 1 check: Spark -> REST catalog -> S3 and back, with both probes wired in.

Creates a tiny table, writes three rows, reads them back through a freshly loaded table
handle, and prints what the probes saw. Drops the table again at the end.

Run from the project root:  bin/submit.sh setup/check_access.py
"""

import json
import os
import sys
import urllib.request

from pyspark.sql import SparkSession

REST_URI = os.environ.get("REST_URI", "http://localhost:8181")
NAMESPACE = "bench.metabench_check"
TABLE = f"{NAMESPACE}.ping"

TIME_UNITS_TO_MS = {"nanoseconds": 1e-6, "microseconds": 1e-3, "milliseconds": 1.0, "seconds": 1e3}


def fail(message):
    print(f"FAIL  {message}")
    sys.exit(1)


def planning_ms(scan_report):
    timer = scan_report["metrics"]["total-planning-duration"]
    return timer["total-duration"] * TIME_UNITS_TO_MS[timer["time-unit"]]


try:
    with urllib.request.urlopen(f"{REST_URI}/v1/config", timeout=5):
        pass
except OSError as e:
    fail(f"REST catalog not reachable at {REST_URI} ({e}). Is the docker compose stack up?")
print(f"ok    REST catalog answers at {REST_URI}")

spark = SparkSession.builder.appName("metabench-check-access").getOrCreate()
probes = spark._jvm.thesis.metabench

spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {NAMESPACE}")
spark.sql(f"DROP TABLE IF EXISTS {TABLE} PURGE")
spark.sql(f"CREATE TABLE {TABLE} (id BIGINT, msg STRING) USING iceberg")
spark.sql(f"INSERT INTO {TABLE} VALUES (1, 'a'), (2, 'b'), (3, 'c')")
details = {r.col_name: r.data_type for r in spark.sql(f"DESCRIBE TABLE EXTENDED {TABLE}").collect()}
location = details.get("Location", "?")
if not location.startswith("s3://"):
    fail(f"table location is {location}, expected s3://...")
print(f"ok    wrote 3 rows to {location}")

# Drop the cached table handle so the read starts from the catalog, as in a new job.
spark.sql(f"REFRESH TABLE {TABLE}")
probes.FetchStats.reset()
probes.CollectingScanReporter.drainJson()

rows = spark.sql(f"SELECT id, msg FROM {TABLE} ORDER BY id").collect()
if [r.id for r in rows] != [1, 2, 3]:
    fail(f"read back {rows}")
print("ok    read the 3 rows back")

fetch = json.loads(probes.FetchStats.snapshotJson())
reports = json.loads(probes.CollectingScanReporter.drainJson())
kinds = fetch["kinds"]

if kinds["manifest_list"]["files"] < 1 or kinds["manifest"]["files"] < 1:
    fail(f"TimingFileIO saw no manifest reads: {fetch}")
print(
    f"ok    TimingFileIO: {kinds['manifest_list']['files']} manifest list + "
    f"{kinds['manifest']['files']} manifest(s), "
    f"{kinds['manifest_list']['bytes'] + kinds['manifest']['bytes']} bytes, "
    f"t_fetch = {fetch['t_fetch_ms']:.1f} ms"
)

if not reports:
    fail("CollectingScanReporter got no ScanReport")
print(f"ok    ScanReport: t_manifests = {sum(planning_ms(r) for r in reports):.1f} ms")

if kinds["table_metadata"]["files"]:
    print(f"note  Spark read metadata.json itself {kinds['table_metadata']['files']} time(s)")
else:
    print("ok    metadata.json was read by the REST server, not by Spark")

# DROP ... PURGE only logs a warning when deleting a file fails, so check one file afterwards.
iceberg_catalog = spark._jsparkSession.sessionState().catalogManager().catalog("bench").icebergCatalog()
table = iceberg_catalog.loadTable(spark._jvm.org.apache.iceberg.catalog.TableIdentifier.parse("metabench_check.ping"))
metadata_file = table.operations().current().metadataFileLocation()

spark.sql(f"DROP TABLE {TABLE} PURGE")
spark.sql(f"DROP NAMESPACE {NAMESPACE}")
if table.io().newInputFile(metadata_file).exists():
    fail(f"dropped the table from the catalog, but its files are still in S3 under {location} "
         "(the IAM user probably lacks s3:DeleteObject)")
print("ok    dropped the check table and deleted its files")

spark.stop()
