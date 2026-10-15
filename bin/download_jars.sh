#!/usr/bin/env bash
# Downloads the Iceberg jars the jobs run with into jars/ and checks them against
# Maven Central's SHA-1 sums. Versions are pinned so runs stay reproducible.
set -euo pipefail

ICEBERG_VERSION="1.10.1"                       # same as the REST catalog image in docker-compose.yaml
SPARK_RUNTIME="iceberg-spark-runtime-4.0_2.13" # matches pyspark 4.0.x (Scala 2.13)
MAVEN="https://repo1.maven.org/maven2/org/apache/iceberg"

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
mkdir -p "$ROOT/jars"

for artifact in "$SPARK_RUNTIME" iceberg-aws-bundle; do
  jar="$artifact-$ICEBERG_VERSION.jar"
  url="$MAVEN/$artifact/$ICEBERG_VERSION/$jar"
  if [[ ! -f "$ROOT/jars/$jar" ]]; then
    curl -fL --progress-bar -o "$ROOT/jars/$jar" "$url"
  fi
  expected="$(curl -fsL "$url.sha1" | cut -d' ' -f1)"
  actual="$(shasum -a 1 "$ROOT/jars/$jar" | cut -d' ' -f1)"
  if [[ "$expected" != "$actual" ]]; then
    echo "SHA-1 mismatch for $jar; delete it and rerun" >&2
    exit 1
  fi
  echo "ok  $jar"
done
