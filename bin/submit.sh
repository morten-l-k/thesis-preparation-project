#!/usr/bin/env bash
# spark-submit with this experiment's settings:
#   - AWS credentials, region and the other settings from .env in the project root (copy
#     .env.example to .env and fill it in). S3FileIO reads the AWS_* variables from the environment.
#   - the Iceberg jars and metabench-probes.jar from jars/ on the driver classpath. In local
#     mode the driver JVM also runs the executors, so no other classpath is needed.
#   - the venv's Python and pyspark. Without PYSPARK_PYTHON, spark-submit picks whatever
#     python3 is first on PATH, and with it that interpreter's pyspark install.
#
# Usage, from anywhere:  <project>/bin/submit.sh <script.py> [args...]
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

ENV_FILE="${ENV_FILE:-$ROOT/.env}"
if [[ ! -f "$ENV_FILE" ]]; then
  echo "Missing $ENV_FILE. Copy .env.example to .env and fill it in." >&2
  exit 1
fi
set -a
# shellcheck disable=SC1090
source "$ENV_FILE"
set +a

: "${AWS_ACCESS_KEY_ID:?set AWS_ACCESS_KEY_ID in $ENV_FILE}"
: "${AWS_SECRET_ACCESS_KEY:?set AWS_SECRET_ACCESS_KEY in $ENV_FILE}"
: "${AWS_REGION:?set AWS_REGION in $ENV_FILE}"
: "${S3_BUCKET_NAME:?set S3_BUCKET_NAME in $ENV_FILE}"
export AWS_DEFAULT_REGION="${AWS_DEFAULT_REGION:-$AWS_REGION}"
REST_URI="${REST_URI:-http://localhost:${REST_PORT:-8181}}"
export REST_URI

export PYSPARK_PYTHON="$ROOT/.venv/bin/python"
export PYSPARK_DRIVER_PYTHON="$PYSPARK_PYTHON"
export SPARK_LOCAL_IP="127.0.0.1"   # local mode only; keeps Spark off the Wi-Fi interface

exec "$ROOT/.venv/bin/spark-submit" \
  --properties-file "$ROOT/conf/spark-defaults.conf" \
  --conf "spark.sql.catalog.bench.uri=$REST_URI" \
  --driver-memory "${SPARK_DRIVER_MEMORY:-4g}" \
  --driver-class-path "$ROOT/jars/*" \
  --driver-java-options "-Dlog4j2.configurationFile=file:$ROOT/conf/log4j2.properties" \
  "$@"
