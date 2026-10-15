"""Creates bench.metabench.events and appends one commit per day, continuing where it left off.

Each commit is one day of synthetic events, written as --files-per-commit Parquet files of
--rows-per-file rows each. event_id is contiguous within a file, so per-file min/max stats let
a point lookup skip every file but one. Manifest merging is off, so each commit adds exactly
one manifest: the "small manifests" layout.

Run from the project root:
  bin/submit.sh --master "local[32]" setup/populate.py --commits 5     # step 2: pilot
  bin/submit.sh --master "local[32]" setup/populate.py --commits 365   # step 3: continue to a year

The data is a pure function of the day number, so an interrupted run can just be started
again. local[32] because writing small files to S3 is bound by request latency, not CPU.
"""

import argparse
import statistics
import time

from pyspark.sql import SparkSession

from inspect_table import report

TABLE = "metabench.events"
FIRST_DAY_EPOCH = 1735689600  # 2025-01-01T00:00:00Z

# 2 + 98 = 100 columns, so every column gets min/max stats under Iceberg's default limit of 100.
N_REF_IDS, N_AMOUNTS, N_CODES, N_LABELS, N_DATES = 30, 30, 15, 15, 8


def column_exprs(day, first_id, rows):
    """SQL expressions for one day. Every value is a hash of event_id, so reruns are identical."""
    exprs = [
        "event_id",
        f"timestamp_seconds({FIRST_DAY_EPOCH + day * 86400}"
        f" + floor((event_id - {first_id}) * 86400 / {rows})) AS event_ts",
    ]

    def add(name, value):
        exprs.append(f"{value} AS {name}")

    def h(modulo):
        # Pseudo-random in [0, modulo), seeded by column position so every column differs.
        return f"pmod(xxhash64(event_id, {len(exprs)}), {modulo})"

    for i in range(1, N_REF_IDS + 1):
        add(f"ref_id_{i:02d}", h(10 ** (3 + i % 5)))
    for i in range(1, N_AMOUNTS + 1):
        add(f"amount_{i:02d}", f"cast({h(10_000_000)} / 100 AS decimal(12,2))")
    for i in range(1, N_CODES + 1):
        add(f"code_{i:02d}", f"cast({h(10 ** (1 + i % 3))} AS int)")
    for i in range(1, N_LABELS + 1):
        add(f"label_{i:02d}", f"concat('label-', cast({h(10 ** (1 + i % 5))} AS string))")
    for i in range(1, N_DATES + 1):
        add(f"date_{i:02d}", f"date_add(DATE'2015-01-01', cast({h(3650)} AS int))")
    return exprs


def day_frame(spark, day, files, rows_per_file):
    rows = files * rows_per_file
    first_id = day * rows
    # range() splits [first_id, first_id + rows) into `files` contiguous blocks, one task each,
    # and with write.distribution-mode=none every task writes exactly one file.
    return (
        spark.range(first_id, first_id + rows, 1, files)
        .withColumnRenamed("id", "event_id")
        .selectExpr(*column_exprs(day, first_id, rows))
    )


def create_table(spark, files, rows_per_file):
    schema = day_frame(spark, 0, 1, 1).schema
    columns = ", ".join(f"{f.name} {f.dataType.simpleString()}" for f in schema.fields)
    spark.sql("CREATE NAMESPACE IF NOT EXISTS bench.metabench")
    spark.sql(
        f"""
        CREATE TABLE bench.{TABLE} ({columns})
        USING iceberg
        PARTITIONED BY (days(event_ts))
        TBLPROPERTIES (
          'format-version' = '2',
          'commit.manifest-merge.enabled' = 'false',
          'write.distribution-mode' = 'none',
          'read.data-planning-mode' = 'local',
          'read.delete-planning-mode' = 'local',
          'metabench.files-per-commit' = '{files}',
          'metabench.rows-per-file' = '{rows_per_file}'
        )
        """
    )


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--commits", type=int, required=True, help="total number of daily commits to reach")
    parser.add_argument("--files-per-commit", type=int, default=300)
    parser.add_argument("--rows-per-file", type=int, default=50)
    parser.add_argument("--recreate", action="store_true", help="drop the table and its files first")
    args = parser.parse_args()

    spark = SparkSession.builder.appName("metabench-populate").getOrCreate()

    if args.recreate:
        spark.sql(f"DROP TABLE IF EXISTS bench.{TABLE} PURGE")
    if not spark.catalog.tableExists(f"bench.{TABLE}"):
        create_table(spark, args.files_per_commit, args.rows_per_file)
        print(f"created bench.{TABLE}")

    props = {r.key: r.value for r in spark.sql(f"SHOW TBLPROPERTIES bench.{TABLE}").collect()}
    existing = (int(props["metabench.files-per-commit"]), int(props["metabench.rows-per-file"]))
    if existing != (args.files_per_commit, args.rows_per_file):
        raise SystemExit(
            f"bench.{TABLE} was built with {existing[0]} files x {existing[1]} rows per commit; "
            "pass the same values to continue it, or --recreate to start over"
        )

    done = spark.sql(f"SELECT count(*) FROM bench.{TABLE}.snapshots WHERE operation = 'append'").first()[0]
    print(f"{done} commits already in the table, writing {max(args.commits - done, 0)} more")

    seconds = []
    for day in range(done, args.commits):
        start = time.perf_counter()
        day_frame(spark, day, args.files_per_commit, args.rows_per_file).writeTo(f"bench.{TABLE}").append() ## Commited the created day_frame
        seconds.append(time.perf_counter() - start)
        print(f"day {day + 1:>3}/{args.commits}  {args.files_per_commit} files  {seconds[-1]:5.1f} s", flush=True)

    report(spark, TABLE, seconds_per_commit=statistics.median(seconds) if seconds else None)
    spark.stop()


if __name__ == "__main__":
    main()
