#!/usr/bin/env bash
set -euo pipefail
curl -s http://localhost:8080/json/ > /tmp/spark.json
python3 - <<'PY'
import json
d = json.load(open("/tmp/spark.json"))
print("workers          =", len(d.get("workers", [])))
print("alive_workers    =", d.get("aliveworkers", None))
print("active_apps      =", [(a["name"], a["id"], a["cores"], a["state"]) for a in d.get("activeapps", [])])
print("completed_apps   =", len(d.get("completedapps", [])))
PY
