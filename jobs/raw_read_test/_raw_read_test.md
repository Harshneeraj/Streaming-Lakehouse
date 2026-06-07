Your question: if you read the raw Parquet files directly, will you find all the records?

The answer differs by table type — and the key insight is what counts as "all records":

COW — raw Parquet contains BOTH versions (across 2 files):

HUDI read    -> 1 row: (A, ts=2, second)      <- correct latest
RAW parquet  -> 2 rows: (A,ts=1,first) + (A,ts=2,second)
files: 2 base parquet, 0 logs
COW rewrites the entire base file on every update. The first batch wrote parquet#1 containing (A, first). The update wrote a brand-new parquet#2 containing the full merged record (A, second) — it did not delete parquet#1. So both files physically exist, each holding one version. Reading raw Parquet across the partition gives you both versions (2 rows). Hudi only returns the latest because its timeline knows parquet#2 is the current file-slice and parquet#1 is an older, superseded version (kept until cleaning removes it).

MOR — raw Parquet contains only the ORIGINAL; the update is in a log file:

HUDI read    -> 1 row: (A, ts=2, second)      <- correct latest (base+log merged)
RAW parquet  -> 1 row: (A, ts=1, first)       <- only the OLD value!
files: 1 base parquet, 1 log file
MOR appends updates to a row-based Avro .log file, not Parquet. The base Parquet still holds the original (A, first). The update (A, second) lives in the .log.1 file. Reading raw Parquet only sees the base file → you get the stale original value, and you'd completely miss the update because it's not in any Parquet file at all.

So, directly answering "will I find all the records":

Raw Parquet shows	Why
COW	All versions (old + new), as separate base files	Each update rewrites a full new base Parquet; old base lingers until cleaned
MOR	Only the original base values; updates are invisible	Updates go to Avro .log files, not Parquet
The broader lesson: raw Parquet reads bypass Hudi's two correctness mechanisms — the timeline (which tells COW which base file is current) and the log merge (which folds MOR deltas into the base). That's why you should never read Hudi tables as plain Parquet: with COW you'd get duplicate/stale versions, and with MOR you'd silently miss every uncompacted update. Only the Hudi-aware reader (or a connector like Trino's Hudi connector) gives the correct single latest row.

If you ran compaction on the MOR table, that .log file would merge into a new base Parquet, and then a raw read would show (A, second) — but until compaction, the raw Parquet is stale.