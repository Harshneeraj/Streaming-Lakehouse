this is Hudi's cleaner service, which removes those old superseded base files. Let me explain the options, then actually demonstrate it removing the stale COW file.

The cleaner is controlled by these configs (you already have them in stream_to_hudi.py):

hoodie.clean.automatic=true — run cleaning automatically after each commit (default true)
hoodie.cleaner.policy=KEEP_LATEST_COMMITS — retention strategy
hoodie.cleaner.commits.retained=N — keep the last N commits' file versions
The catch: in my raw-read test I set hoodie.clean.automatic=false specifically so old files would linger for inspection. With default settings the cleaner keeps the last 10 commits, so old base files survive 10 updates before being purged. To see cleaning actually delete a file, I'll set commits.retained=1. Let me build that test.

Command
c:\Users\Harsh Kumar\Desktop\Try\Streaming-Lakehouse

wsl bash -c "docker exec minio mc alias set local http://localhost:9000 admin admin12345 >/dev/null 2>&1; docker exec minio mc rm --recursive --force local/lakehouse/_test >/dev/null 2>&1; make spark job=jobs/clean_test/_clean_test.py 2>&1 | grep -E 'after batch|hudi read|Error|Exception' | head -20"
[after batch 1] raw parquet rows = 1
[after batch 2] raw parquet rows = 2
[after batch 3] raw parquet rows = 2
[hudi read] 1 row(s): (ts=3,val=third)
There's the cleaner working. Watch the raw-parquet row count:

after batch 1: 1 raw row    (1 base file: ts=1)
after batch 2: 2 raw rows   (2 base files: ts=1, ts=2 — old not yet cleaned)
after batch 3: 2 raw rows   (ts=1 CLEANED, but ts=3 added -> ts=2, ts=3)
hudi read:     1 row (ts=3) (always correct)
Between batch 2 and batch 3 the count stayed at 2 instead of climbing to 3 — that's the cleaner deleting the oldest base file (ts=1) once it fell outside the retained=1 window. Without cleaning it would have been 3. So cleaning is what reclaims that stale-file space.

Now, how to clean COW tables in practice — three approaches:

1. Automatic (inline) cleaning — the default, already in your pipeline

Your stream_to_hudi.py already does this:

"hoodie.clean.automatic": "true",
"hoodie.clean.async": "true",
"hoodie.cleaner.policy": "KEEP_LATEST_COMMITS",
"hoodie.cleaner.commits.retained": "10",
After each commit, Hudi cleans base files older than the last 10 commits. async=true means it runs in a background thread so it doesn't block writes. This is the recommended setup for streaming — you don't do anything, it self-maintains. To clean more aggressively, lower commits.retained (e.g. to 1–3); to keep more history for time-travel, raise it.

2. Retention policy choice

KEEP_LATEST_COMMITS (what you use) — keeps file versions from the last N commits. Good for time-travel within that window.
KEEP_LATEST_FILE_VERSIONS — keeps the last N file versions per file group regardless of commits. Use hoodie.cleaner.fileversions.retained.
3. Standalone / on-demand cleaning (without a write)

If you want to clean an existing table without running an ingest job, run the Hudi cleaner directly. Two common ways:

Via Spark SQL call procedure (Hudi 0.11+):

CALL run_clean(table => 'lake.orders_cow', retain_commits => 3);
Or the standalone HoodieCleaner utility:

spark-submit --class org.apache.hudi.utilities.HoodieCleaner \
  /path/hudi-utilities-bundle.jar \
  --target-base-path s3a://lakehouse/orders/orders_cow \
  --hoodie-conf hoodie.cleaner.policy=KEEP_LATEST_COMMITS \
  --hoodie-conf hoodie.cleaner.commits.retained=3
Important caveat: cleaning is not the same as the partition-move delete we discussed earlier. Cleaning removes old superseded versions of files to reclaim space and bound the timeline — it never removes the current/latest data. It also directly trades off against time travel: once a commit's files are cleaned, you can no longer time-travel to that instant.

For your project, you're already set — automatic async cleaning with 10 retained commits is a sane streaming default. If disk on MinIO is a concern, drop commits.retained to something like 3; if you want a longer time-travel window, raise it.