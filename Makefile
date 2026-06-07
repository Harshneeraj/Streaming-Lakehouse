SHELL := /bin/bash

COMPOSE := docker compose
PROJECT := hudi-pipeline

.PHONY: help build up down clean produce ingest verify logs ps trino sql kafka-ui ui status smoke spark

help:
	@echo "Targets:"
	@echo "  make build      - build custom images (spark, hive-metastore, producer)"
	@echo "  make up         - start kafka, minio, postgres, hive-metastore, spark, trino"
	@echo "  make down       - stop and remove containers (volumes preserved)"
	@echo "  make clean      - down + drop volumes (full reset)"
	@echo "  make produce    - run the synthetic event producer (foreground)"
	@echo "  make ingest             - submit the streaming job (foreground)"
	@echo "  make ingest mode=bg     - submit the streaming job detached (background)"
	@echo "  make verify     - run the Hudi verification job"
	@echo "  make spark job=path/to/job.py [args=\"...\"] - submit any Spark job under jobs/"
	@echo "  make smoke      - run the end-to-end smoke test (counts, aggregates, layout)"
	@echo "  make logs s=...   - tail logs for service s (e.g. s=spark-master)"
	@echo "  make trino      - open a trino CLI"
	@echo "  make sql [file=path/to.sql] - run a sql file against trino (default docs/queries.sql)"
	@echo "  make ui         - print all UI URLs"

build:
	$(COMPOSE) build

up:
	$(COMPOSE) up -d kafka kafka-ui minio minio-init postgres hms-deps hive-metastore spark-master spark-worker trino
	@echo ""
	@echo "Waiting for hive-metastore to become healthy..."
	@for i in $$(seq 1 60); do \
	  status=$$($(COMPOSE) ps --format json hive-metastore | grep -oE '"Health":"[a-z]+"' | head -1); \
	  if echo $$status | grep -q healthy; then echo "hive-metastore healthy"; break; fi; \
	  sleep 2; \
	done
	@$(MAKE) ui

down:
	$(COMPOSE) down

clean:
	$(COMPOSE) down -v --remove-orphans

produce:
	$(COMPOSE) --profile producer run --rm producer

# Submit the streaming job. Default runs in the foreground.
# Usage:
#   make ingest            - run in the foreground (default)
#   make ingest mode=bg    - run detached in the background
ingest:
	@if [ "$(mode)" = "bg" ] || [ "$(mode)" = "background" ]; then \
	  $(COMPOSE) exec -d spark-master bash -lc '\
	    nohup /opt/bitnami/spark/bin/spark-submit \
	      --master spark://spark-master:7077 \
	      --deploy-mode client \
	      --name kafka_to_hudi \
	      --conf spark.driver.host=spark-master \
	      /opt/jobs/streaming/stream_to_hudi.py \
	      > /tmp/stream.log 2>&1 &'; \
	  echo "Streaming job submitted in background. Tail with: docker compose exec spark-master tail -f /tmp/stream.log"; \
	else \
	  $(COMPOSE) exec spark-master /opt/bitnami/spark/bin/spark-submit \
	    --master spark://spark-master:7077 \
	    --deploy-mode client \
	    --name kafka_to_hudi \
	    --conf spark.driver.host=spark-master \
	    /opt/jobs/streaming/stream_to_hudi.py; \
	fi

verify:
	$(COMPOSE) exec spark-master /opt/bitnami/spark/bin/spark-submit \
	  --master spark://spark-master:7077 \
	  --deploy-mode client \
	  --name verify_hudi \
	  --conf spark.driver.host=spark-master \
	  /opt/jobs/streaming/verify_hudi.py

# Submit an arbitrary Spark job living under ./jobs (mounted at /opt/jobs).
# Usage:
#   make spark job=jobs/my_job.py
#   make spark job=my_job.py name=my_job args="--foo bar --baz 1"
spark:
	@if [ -z "$(job)" ]; then \
	  echo "ERROR: job is required. Usage: make spark job=jobs/my_job.py [name=my_job] [args=\"--foo bar\"]"; \
	  exit 1; \
	fi; \
	rel="$(job)"; rel="$${rel#jobs/}"; rel="$${rel#/opt/jobs/}"; \
	jobname="$(name)"; [ -z "$$jobname" ] && jobname="$$(basename $$rel .py)"; \
	echo "Submitting /opt/jobs/$$rel as '$$jobname'..."; \
	$(COMPOSE) exec spark-master /opt/bitnami/spark/bin/spark-submit \
	  --master spark://spark-master:7077 \
	  --deploy-mode client \
	  --name "$$jobname" \
	  --conf spark.driver.host=spark-master \
	  "/opt/jobs/$$rel" $(args)

logs:
	$(COMPOSE) logs -f --tail=200 $(s)

ps:
	$(COMPOSE) ps

trino:
	$(COMPOSE) exec trino trino --server http://localhost:8080 --catalog hudi --schema lake

# Run a .sql file against trino. Defaults to docs/queries.sql.
# Usage:
#   make sql                              - run docs/queries.sql
#   make sql file=jobs/streaming/x.sql    - run a specific file
sql:
	@sqlfile="$(file)"; [ -z "$$sqlfile" ] && sqlfile="docs/queries.sql"; \
	if [ ! -f "$$sqlfile" ]; then echo "ERROR: file not found: $$sqlfile"; exit 1; fi; \
	echo "Running $$sqlfile against trino..."; \
	$(COMPOSE) exec -T trino trino --server http://localhost:8080 --catalog hudi --schema lake < "$$sqlfile"

smoke:
	bash scripts/smoke_test.sh

ui:
	@echo ""
	@echo "  Kafka UI       : http://localhost:8088"
	@echo "  MinIO Console  : http://localhost:9001  (admin / admin12345)"
	@echo "  Spark Master   : http://localhost:8080"
	@echo "  Spark Worker   : http://localhost:8081"
	@echo "  Spark App UI   : http://localhost:4040  (only while a job is running)"
	@echo "  Trino UI       : http://localhost:8090"
	@echo ""

status:
	@$(COMPOSE) ps
