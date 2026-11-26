#!/usr/bin/env bash
# Regenerates the format-v3 fixture tables with Spark 4 + Apache Iceberg (needs Docker).
# The warehouse path is fixed: the table metadata stores absolute file paths.
set -euo pipefail
here="$(cd "$(dirname "$0")" && pwd)"
warehouse=/tmp/faucet-fixtures/iceberg-v3
rm -rf "$here/warehouse" && mkdir -p "$here/warehouse"
docker run --rm --user 0 \
  -v "$here/warehouse:$warehouse" -v "$here/tables.sql:/tables.sql:ro" \
  apache/spark:4.0.1 /opt/spark/bin/spark-sql \
  --master 'local[1]' \
  --packages org.apache.iceberg:iceberg-spark-runtime-4.0_2.13:1.10.0,org.xerial:sqlite-jdbc:3.50.3.0 \
  --conf spark.jars.ivy=/tmp/ivy \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  --conf spark.sql.catalog.fx=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.fx.type=jdbc \
  --conf spark.sql.catalog.fx.uri=jdbc:sqlite:/tmp/catalog.db \
  --conf spark.sql.catalog.fx.warehouse="$warehouse" \
  --conf spark.sql.shuffle.partitions=1 \
  -f /tables.sql
find "$here/warehouse" -name '.*.crc' -delete
(cd "$here" && find warehouse -type f | LC_ALL=C sort | xargs shasum -a 256 > SHA256SUMS)
