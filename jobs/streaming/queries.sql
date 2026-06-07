-- A sampler of Trino queries against the Hudi catalogue.
-- Run with: make sql

SHOW SCHEMAS FROM hudi;
SHOW TABLES FROM hudi.lake;

-- COW snapshot
SELECT COUNT(*) AS rows_cow FROM hudi.lake.orders_cow;

-- MOR snapshot (default) and read-optimized: Trino's hudi connector serves snapshot
SELECT COUNT(*) AS rows_mor FROM hudi.lake.orders_mor;

-- Aggregations - hits Hudi metadata column stats for predicate pushdown
SELECT country, COUNT(*) AS n, ROUND(AVG(amount), 2) AS avg_amount
FROM hudi.lake.orders_cow
GROUP BY country
ORDER BY n DESC;

SELECT status, COUNT(*) AS n
FROM hudi.lake.orders_cow
GROUP BY status
ORDER BY n DESC;

SELECT category, currency, COUNT(*) AS n, ROUND(SUM(amount), 2) AS total
FROM hudi.lake.orders_cow
WHERE country IN ('IN', 'US')
GROUP BY category, currency
ORDER BY total DESC
LIMIT 25;
