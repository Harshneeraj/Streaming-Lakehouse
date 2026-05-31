#!/usr/bin/env bash
# End-to-end smoke check for the Kafka → Spark → Hudi pipeline.
#
# Doesn't bail on the first failed query; reports per-section pass/fail and a final
# summary. Drops the noisy JVM warning lines from Trino's CLI.
set -uo pipefail

SERVER=${TRINO_SERVER:-http://localhost:8080}
TRINO="docker exec -i trino trino --server $SERVER"
MC="docker exec minio mc"
NOISE='WARNING\|JAVA_TOOL\|jline\|^$'

PASS=0
FAIL=0

banner() {
  printf '\n── %s ──────────────────────────────────────────────\n' "$1"
}

run_trino() {
  # $1 = description, $2 = catalog, $3 = schema (or empty), $4 = SQL
  local desc=$1 cat=$2 sch=$3 sql=$4 args=()
  args=(--catalog "$cat")
  [ -n "$sch" ] && args+=(--schema "$sch")
  if out=$($TRINO "${args[@]}" --execute "$sql" 2>&1); then
    echo "$out" | grep -v "$NOISE" | sed '/^$/d'
    PASS=$((PASS+1))
  else
    echo "FAIL: $desc"
    echo "$out" | tail -5
    FAIL=$((FAIL+1))
  fi
}

wait_for_trino() {
  local n=0
  while [ $n -lt 30 ]; do
    if $TRINO --execute 'SELECT 1' >/dev/null 2>&1; then
      return 0
    fi
    n=$((n+1))
    sleep 2
  done
  return 1
}

echo "Waiting for Trino to be ready..."
if ! wait_for_trino; then
  echo "Trino did not become ready in 60s. Check 'docker logs trino'."
  exit 2
fi
echo "Trino is ready."

banner "1. catalogs"
run_trino "list catalogs" system "" "SHOW CATALOGS"

banner "2. tables in lake"
run_trino "list tables" hudi lake "SHOW TABLES"

banner "3. row counts (COW + MOR variants)"
run_trino "row counts" hudi lake \
  "SELECT 'orders_cow'    AS tbl, COUNT(*) AS n FROM orders_cow
   UNION ALL SELECT 'orders_mor'    , COUNT(*) FROM orders_mor
   UNION ALL SELECT 'orders_mor_ro' , COUNT(*) FROM orders_mor_ro
   UNION ALL SELECT 'orders_mor_rt' , COUNT(*) FROM orders_mor_rt"

banner "4. status histogram (proves upserts worked)"
run_trino "status histogram" hudi lake \
  "SELECT status, COUNT(*) n FROM orders_cow GROUP BY status ORDER BY 2 DESC"

banner "5. partition pruning sanity (country='IN')"
run_trino "partition pruning" hudi lake \
  "SELECT category, COUNT(*) n, ROUND(AVG(amount), 2) avg_amount
   FROM orders_cow WHERE country = 'IN' GROUP BY category ORDER BY 2 DESC"

banner "6. MinIO inventory of partitions"
if out=$($MC ls local/lakehouse/orders/orders_cow/ 2>&1); then
  echo "$out" | head -20
  PASS=$((PASS+1))
else
  echo "FAIL: minio ls"
  echo "$out"
  FAIL=$((FAIL+1))
fi

banner "7. Hudi commit timeline"
if out=$($MC ls local/lakehouse/orders/orders_cow/.hoodie/ 2>&1); then
  echo "$out" | grep -E '\.(commit|deltacommit)$' | head -10 || true
  PASS=$((PASS+1))
else
  echo "FAIL: minio ls .hoodie"
  echo "$out"
  FAIL=$((FAIL+1))
fi

echo
printf '── summary ─────────────────────────  pass=%d  fail=%d\n' "$PASS" "$FAIL"
[ "$FAIL" -eq 0 ]
