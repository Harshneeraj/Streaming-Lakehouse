SHELL := /bin/bash
COMPOSE := docker compose

.PHONY: help build up up-standalone down clean test demo \
        ex1 ex2 ex3 ex4 ex5 shell

help:
	@echo "Targets:"
	@echo "  make build         build the python worker image"
	@echo "  make up            start the worker (uses main project's MinIO at minio:9000)"
	@echo "  make up-standalone start worker + dedicated MinIO (host ports 9100/9101)"
	@echo "  make down          stop the worker"
	@echo "  make clean         stop + drop volumes"
	@echo "  make test          run library tests inside the worker"
	@echo "  make demo          run all examples end-to-end"
	@echo "  make ex1..ex5      run a specific example"
	@echo "  make shell         open a python shell inside the worker"

build:
	$(COMPOSE) build app

up:
	$(COMPOSE) up -d app

up-standalone:
	$(COMPOSE) --profile standalone up -d
	$(COMPOSE) up -d app
	@sleep 2
	@docker exec mi-app sh -c 'echo using $$ICEBERG_S3_ENDPOINT'

down:
	$(COMPOSE) --profile standalone down

clean:
	$(COMPOSE) --profile standalone down -v --remove-orphans

test:
	docker exec -i mi-app python /app/tests/test_table.py

ex1:
	docker exec -i mi-app python /app/examples/01_write_and_read.py

ex2:
	docker exec -i mi-app python /app/examples/02_time_travel.py

ex3:
	docker exec -i mi-app python /app/examples/03_schema_evolution.py

ex4:
	docker exec -i mi-app python /app/examples/04_partitioned.py

ex5:
	docker exec -i mi-app python /app/examples/05_query_with_duckdb.py

demo: ex1 ex2 ex3 ex4 ex5

shell:
	docker exec -it mi-app python
