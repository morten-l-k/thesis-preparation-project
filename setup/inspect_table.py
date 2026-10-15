"""Live metadata and data volume of an Iceberg table, and what those numbers extrapolate to.

"Live" means reachable from the current snapshot, which is what a query actually reads.
Prints a summary and saves it to results/table-<name>-<snapshot id>.json.

Run from the project root:  bin/submit.sh setup/inspect_table.py [--table metabench.events]
populate.py calls report() itself after writing.
"""

import argparse
import json
import math
from pathlib import Path

from pyspark.sql import SparkSession

RESULTS = Path(__file__).resolve().parents[1] / "results"
TARGET_MANIFEST_MB = (50, 100, 200, 300)


def size(n):
    for unit in ("B", "KB", "MB", "GB"):
        if n < 1024 or unit == "GB":
            return f"{n:,.0f} {unit}" if unit == "B" else f"{n:,.1f} {unit}"
        n /= 1024


def report(spark, table, seconds_per_commit=None, extrapolate=True):
    """table is namespace.name inside the bench catalog."""
    name = f"bench.{table}"
    files = spark.sql(
        f"SELECT count(*) AS n, sum(file_size_in_bytes) AS bytes, sum(record_count) AS rows FROM {name}.files"
    ).first()
    manifests = spark.sql(
        f"SELECT count(*) AS n, sum(length) AS bytes,"
        f" sum(coalesce(added_data_files_count, 0) + coalesce(existing_data_files_count, 0)) AS entries"
        f" FROM {name}.manifests"
    ).first()
    if not files.n:
        print(f"\n{name} has no data files yet")
        return None
    commits = spark.sql(f"SELECT count(*) FROM {name}.snapshots WHERE operation = 'append'").first()[0]
    props = {r.key: r.value for r in spark.sql(f"SHOW TBLPROPERTIES {name}").collect()}

    # The manifest list and metadata.json sizes aren't in any metadata table; ask the FileIO.
    catalog = spark._jsparkSession.sessionState().catalogManager().catalog("bench").icebergCatalog()
    jtable = catalog.loadTable(spark._jvm.org.apache.iceberg.catalog.TableIdentifier.parse(table))
    snapshot = jtable.currentSnapshot()
    io = jtable.io()
    manifest_list_bytes = io.newInputFile(snapshot.manifestListLocation()).getLength()
    metadata_json_bytes = io.newInputFile(jtable.operations().current().metadataFileLocation()).getLength()

    bytes_per_entry = manifests.bytes / manifests.entries
    bytes_per_file = files.bytes / files.n
    stats = {
        "table": table,
        "snapshot_id": snapshot.snapshotId(),
        "commits": commits,
        "data_files": files.n,
        "data_bytes": files.bytes,
        "rows": files.rows,
        "manifests": manifests.n,
        "manifest_bytes": manifests.bytes,
        "manifest_entries": manifests.entries,
        "manifest_list_bytes": manifest_list_bytes,
        "metadata_json_bytes": metadata_json_bytes,
        "properties": {k: v for k, v in props.items() if not k.startswith(("current-", "snapshot-count"))},
    }
    RESULTS.mkdir(exist_ok=True)
    out = RESULTS / f"table-{table.replace('.', '_')}-{stats['snapshot_id']}.json"
    out.write_text(json.dumps(stats, indent=2))

    print(f"\n{name} at snapshot {stats['snapshot_id']} ({commits} commits)")
    print(f"  data files     {files.n:>9,}  {size(files.bytes):>10}  "
          f"{size(bytes_per_file)} and {files.rows / files.n:,.0f} rows per file")
    print(f"  manifests      {manifests.n:>9,}  {size(manifests.bytes):>10}  "
          f"{size(bytes_per_entry)} per data file entry")
    print(f"  manifest list  {'':>9}  {size(manifest_list_bytes):>10}")
    print(f"  metadata.json  {'':>9}  {size(metadata_json_bytes):>10}")
    if commits and files.n % commits:
        print(f"  WARNING: {files.n} files over {commits} commits is not a whole number per commit")
    if not extrapolate:
        print(f"\n  saved {out.relative_to(RESULTS.parent)}")
        return stats

    files_per_commit = int(props.get("metabench.files-per-commit", 0)) or round(files.n / max(commits, 1))
    print(f"\n  extrapolated at {files_per_commit} files per commit:")
    print(f"  {'live manifests':>14}  {'data files':>10}  {'commits':>7}  {'data in S3':>10}  {'fill time':>9}")
    for target_mb in TARGET_MANIFEST_MB:
        n_files = target_mb * 1024 * 1024 / bytes_per_entry
        n_commits = math.ceil(n_files / files_per_commit)
        fill = f"{n_commits * seconds_per_commit / 60:,.0f} min" if seconds_per_commit else "-"
        print(f"  {target_mb:>11} MB  {n_files:>10,.0f}  {n_commits:>7,}  {size(n_files * bytes_per_file):>10}  {fill:>9}")
    if seconds_per_commit:
        print(f"  (fill time assumes the median commit time seen here, {seconds_per_commit:.1f} s)")
    print(f"\n  saved {out.relative_to(RESULTS.parent)}")
    return stats


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--table", default="metabench.events", help="namespace.name in the bench catalog")
    args = parser.parse_args()
    spark = SparkSession.builder.appName("metabench-inspect").getOrCreate()
    report(spark, args.table)
    spark.stop()
