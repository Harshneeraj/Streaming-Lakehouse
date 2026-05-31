#!/usr/bin/env bash
set -euo pipefail
curl -s http://localhost:8080/json/ > /tmp/spark.json
ids=$(python3 -c 'import json; d=json.load(open("/tmp/spark.json")); print(" ".join(a["id"] for a in d.get("activeapps", [])))')
if [ -z "${ids// }" ]; then
  echo "no active apps"
  exit 0
fi
for id in $ids; do
  echo "killing $id"
  curl -s -XPOST "http://localhost:8080/app/kill/?id=${id}&terminate=true" -o /dev/null
done
sleep 3
bash "$(dirname "$0")/spark_status.sh"
