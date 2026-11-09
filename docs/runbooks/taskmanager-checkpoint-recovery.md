# TaskManager Checkpoint Recovery — Controlled Local Drill

## Purpose

Demonstrate that the existing user_play_count_1h job can recover from a
TaskManager process failure and continue producing the expected feature value.

This is a manually executed local failure drill, not an automated recovery test.

## Failure boundary

The TaskManager is killed with SIGKILL and restarted.

The JobManager and Kafka remain running. The job is not resubmitted.
This drill does not cover JobManager loss, cluster recreation, durable recovery
after loss of JobManager checkpoint storage, or a Redis materializer failure.

## Isolation and prerequisites

- Use fresh, dedicated replay topics and unique event identities.
- Use one Kafka partition and Flink parallelism one.
- Confirm no unrelated jobs are running before interrupting the TaskManager.
- Start the existing feature job without --bounded.
- Do not modify Project 1 or reuse the historical replay's output namespace.
- Keep the test within the deduplication TTL.
- Do not repeat a partially completed producer step without inspecting offsets
  and its local intent file.

## Test protocol

1. Create isolated topics using scripts/create_replay_topics.py.
2. Submit jobs/user_play_count_1h.py with explicit input, feature, late, group,
   and job-name arguments.
3. Publish one play for a selected user and observation window.
4. Publish a distinct non-play progress event eleven minutes after the target
   observation time.
5. Observe count 1 for the target window.
6. Wait for a successful checkpoint whose trigger timestamp is later than the
   baseline observation.
7. Record the job ID, baseline output offset, checkpoint ID, and restore count.
8. Kill only the TaskManager and restart it.
9. Confirm the same job ID is RUNNING and all vertices are RUNNING.
10. Confirm the restore count increased and the restored checkpoint ID is at
    least the previously recorded baseline checkpoint ID.
11. Capture the current feature-output end offset.
12. Publish the exact original play again, a second distinct play in the same
    target window, and the original progress event again.
13. Observe new target-window output matching the independent reference count 2.
14. Continue observing for fifteen seconds and reject counts above 2.
15. Verify the last observed target-window count is 2 and the late topic is empty.
16. Record the result and cancel the isolated job.

Repeating the progress event can advance timestamp/watermark generation even
though its logical duplicate is suppressed before window aggregation.

## Failure injection commands

Only after the baseline and completed checkpoint have been recorded:

```bash
docker compose kill -s SIGKILL taskmanager
docker compose up -d taskmanager
```

Do not restart the JobManager or submit another job during this drill.

## Recorded evidence — November 9, 2026

| Evidence | Observed result |
|---|---|
| Run | ff-recovery-20261109t092412z |
| Job ID | dd64af55c6801ca9784e2bec0c9fbf1e |
| Target observation | 2026-11-09T09:29:00Z |
| Baseline count / output offset | 1 / 1 |
| Checkpoint completed after baseline observation | 18 |
| Baseline checkpoint state size | 36,140 bytes |
| Restored checkpoint | 23 |
| Restore count | 0 → 1 |
| Vertices after restore | All three RUNNING |
| Input range before / after probe | [0, 2) / [0, 5) |
| New target-window output offset | 2 |
| Reference / observed count after probe | 2 / 2 |
| Additional observation period | 15 seconds |
| Count above 2 during observation | None observed |
| Late output range after probe | [0, 0) |
| Checkpoint totals after probe | 39 completed, 1 failed, 1 restore |

Checkpoint 23 is later than the baseline checkpoint because periodic checkpoints
continued before the failure.

Local evidence was written under:

    output/stream_snapshots/recovery/ff-recovery-20261109t092412z/

Artifacts:

- fixture.json
- before-failure.json
- after-restore.json
- probe-intent.json
- after-recovery-probe.json

Generated artifacts remain outside Git. This document records the inspected
result; it does not make the local artifacts available in a clean clone.

## Interpretation and limitations

The REST evidence establishes an actual checkpoint restore, not merely a
transition back to RUNNING.

The combined duplicate-and-new-event probe establishes the expected functional
result for the selected user/window after that restore. It does not separately
isolate every operator's restored state or independently inspect restored source
offsets.

The Kafka sink remains AT_LEAST_ONCE. Repeated output records are possible.
The bounded observation rejects inflated feature values, not all duplicate
deliveries or every possible later failure.

Actual watermark scheduling is not independently reconstructed. No claim is
made about unlimited replay protection, end-to-end exactly-once processing,
whole-cluster recovery, or production readiness.
