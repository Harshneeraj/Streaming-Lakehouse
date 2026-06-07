Q1: Time travel after the old record is cleaned — NO. (Fully verified.)

This one I proved conclusively. With cleaner.commits.retained=1, time-travel to the oldest commit failed with Hudi's own error:

"Cleaner cleaned up the timestamp of interest. Please ensure sufficient commits are retained with cleaner for Timestamp as of query to work"

…while time-travel to the latest commit succeeded. So once cleaning removes a commit's base files, that historical snapshot is gone — the .commit metadata may still list the instant, but the data needed to reconstruct it is deleted. Your time-travel window equals your cleaner retention window (hoodie.cleaner.commits.retained). To time-travel further back, retain more commits.

Q2: Compaction without writing new data — YES, but I didn't get a clean proof on this stack.

The conceptual answer is yes and it's not in doubt: compaction is a standalone table service. You invoke it without ingesting data via:

CALL run_compaction(op => 'run', table => 'lake.orders_mor') in Spark SQL
the standalone HoodieCompactor utility
offline async compaction
What I could not cleanly demonstrate here, and I want to be honest about it:

First attempts failed because run_compaction pulls in the FileSystemBasedLockProvider, which S3A/MinIO can't support (no atomic creates) — the same limitation your stream_to_hudi.py comments call out.
After working around the lock by disabling the metadata table, the procedure ran without error but produced no compaction instant — the timeline only ever showed .deltacommits, no .commit. The "correct" read result was just MOR's normal read-time merge, not evidence of compaction. My intermediate claim that "compaction ran standalone and succeeded" was wrong, and I corrected it once I checked the timeline.
The most likely reason it stayed a no-op: the compaction plan needs enough accumulated deltas, and the run_compaction procedure builds its own write client that wasn't honoring the inline threshold overrides I passed. Getting it to actually emit a compaction commit on this MinIO/S3A setup would need the right lock + compaction-plan config wired into the procedure's write client — which I haven't nailed down.

So: compaction without new data is definitely possible in Hudi (the API is designed for exactly that), but on this S3A-backed dev stack the standalone procedure needs lock-provider and compaction-trigger config that I haven't fully resolved. Earlier in this session, inline compaction (hoodie.compact.inline=true) on a normal write did work and produced a real compacted base file — that's the verified path here. If you want, I can dig into wiring the in-process lock into the run_compaction procedure properly so we get a genuine standalone compaction commit, or check the Hudi 0.15 docs for the exact procedure parameters. Which would you prefer?