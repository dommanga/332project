# Week 4 Progress (Nov 3 - Nov 9, 2025)

## This Week's Progress

### What We Completed

#### 1. Splitters. Plan & Broadcast (Youngseo)

- **SamplingCoordinator**
  - Perworker stream buffer, "all-done" readiness, splitter handoff
- **SplitterCalculator**
  - Unsigned 10-byte key sort, **quantile splitters** (k-1)
- **PartitionPlanner**
  - (k-1) splitters -> **k ranges** with `[lo, hi)` semantics
  - edge cases: no splitters (k=1), all-min/all-max boundaries
- **MasterServer integration**
  - On "splitters ready" -> build `PartitionPlan`
  - **Broadcast to all workers** via `WorkerService.SetPartitionPlan`
  -single-shot guard with `AtomicBoolean`
- **Tests**
  - `PartitionPlannerSpec` (boundaries + k=1)
  - All test: 5/5 passed


#### 2. Worker Client & Sampling (tedoh7)

- **WorkerClient.scala**
  - CLI parser (`--master`, `-I`, `-O`, `--id`, `--port`) & defaults
  - Registers to Master, sends samples via client streaming
  - Displays splitters received from Master
- **worker/Sampling.scala**
  - Every-N key sampling from 100-byte records (10-byte unsigned keys)
  - Local file validation & simple logging
- Local build verified: `sbt compile` OK

#### 3. Master-side Core (Jimi)

- **WorkerRegistry.scala**
  - Unique worker ID assignment, livenessmap, lastBeat tracking
  - Heartbeat timeout (30s) + background prune thread
  - Dummy partition ids (3 per worker) for early wiring
- **MasterServer.scala**
  - gRPC server lifecycle (start/stop/shutdown hooks)
  - `registerWorker()` -> assignment + ordering log
  - `heartbeat()` -> liveness update + ack
  - `sendSamples()` -> client-stream ingest (per-worker), completion barrier, reply with splitters
  - Integrated with **SamplingCoordinator** (from Youngseo) for sample aggregation
- **MasterClient.scala**
  - (For Worker->master comms) Registration + client-stream sample push
  - Splitter response handling
  - Connection liftcycle logs

#### 4. Documentation & Repo Hygiene

- Updated `README.md` with Week 4 milestone
- Strengthened `docs/Week4.md` with run order, port-conflict notes, trouleshooting.

```
sbt -Dsbt.global.base="$HOME/.sbt-global-clean" clean compile test
sbt -Dsbt.global.base="$HOME/.sbt-global-clean" "runMain master.MasterServer 2 5001"
sbt -Dsbt.global.base="$HOME/.sbt-global-clean" "runMain worker.WorkerServer 6000"
sbt -Dsbt.global.base="$HOME/.sbt-global-clean" "runMain worker.WorkerServer 6001"
sbt -Dsbt.global.base="$HOME/.sbt-global-clean" \
  "runMain worker.WorkerClient --master 127.0.0.1:5001 -I ./input1/data1.dat -O ./output1 --id worker0 --port 6000"
sbt -Dsbt.global.base="$HOME/.sbt-global-clean" \
  "runMain worker.WorkerClient --master 127.0.0.1:5001 -I ./input2/data2.dat -O ./output2 --id worker1 --port 6001"

```

## Challenges/Issues

### Technical Challenges

- **Broadcast duplication** race -> fixed with `AtomicBoolean.compareAndSet`
- JDK 21/24 `Unsafe` / native-access warnings are non-blocking; documented.

### Team Coordination

- Kept **additive, backward-compatible IDL** for parallel dev.
- Synchronized common logs/flags/run scripts.

## Next Week's Goals (Week 5) — Distributed Operation

### Team Goals – Milestone #3

#### 1. Control Plane (Jimin)
*— Minimal fault-tolerance + reliable delivery*

- Keep WorkerRegistry running (heartbeat updates, basic pruning) and surface active/dead in logs.
- Add simple retry for master→worker broadcasts (e.g., `SetPartitionPlan`) with bounded attempts + short backoff; print success/failure per worker.
- Standardize Ack usage (ok/message) and include a lightweight traceId in logs for broadcast/sendSamples rounds.
- Deliverables: master continues stably with 2–3 workers; transient RPC failures auto-retry and succeed; clear logs for state & retries.

#### 2. Data Plane (Sangwon)
*— Local sort/partition + worker↔worker shuffle*

- Local partitioning by `[lo, hi)` using received splitters → write sorted blocks per target partition; emit `BlockMeta(size, checksum, num_records)`.
- PushPartition (client): stream `PartitionChunk` with increasing seq; handle backpressure; retry dropped chunks.
- PushPartition (server): validate seq, reassemble payload, verify checksum, commit temp→final file atomically.
- Deliverables: in a 2-worker run, each side sends/receives at least one partition; record counts & checksums match.

#### 3. Global Merge & Output (Youngseo)
*— k-way merge + integrity + observability

- Implement k-way merge per final partition from received sorted blocks; write final outputs to agreed path/format.
- Integrity checks: key-range continuity (no gaps/overlaps), global non-decreasing order, total record count reconciliation.
- Observability: per task/partition log merge time, block count, throughput, and a brief size histogram.
- Deliverables: small dataset yields a globally sorted result with matching counts/checksums and timing stats in logs.

#### 4. Integration Test

- Scenario: Master + 2 (or 3) workers.
- Verify end-to-end: **register → heartbeat → samples → splitters → plan broadcast → local sort/partition → shuffle → k-way merge → final output**.
- Validate logs: number of samples, splitters, partitions exchanged, retries (if any), merge stats, and final integrity checks.

### Individual Responsibilities

#### Jimin

- Makes the system robust: heartbeats, state machine, prune logic.
- Implements retry/backoff and clear error messages on RPC failures.
- Provides simple liveness APIs for other components to query.

#### Sangwon

- Delivers worker data path: bucket → sort → write → shuffle.
- Implements `PushPartition` streaming with seq/checksum/backpressure.
- Produces BlockMeta that Master/merge can trust.

#### Youngseo

- Owns the back half: partition merge spec + output correctness.
- Adds master-side metrics & tracing so we can see timing and skew.
- Ensures end-to-end invariants: no overlap, no gaps, duplicates policy defined.

### Cross-Team Interfaces
- **Plan -> Worker**: `SetPartitionPlan(PartitionPlan)`.
- **Shuffle**: `PushPartition(stream PartitionChunk) -> Ack`
  - Required fields in `PartitionChunk`: `task`, `partition_id`, `payload(100B*n)`, `seq`.
  - `Ack.msg` must carry last received `seq` on error.
- **BlockMeta**: (`block_id`, `path`, `size`, `checksum`,[, `num_records`])
- **Tracing**: `task_id` propagated inside `TaskId` for all RPCs.

### PR Checklist
- Unit test added or updated.
- Logs include `task_id` and worker id.
- No blocking `Thread.sleep` in hot paths (except controlled waits).
- Handle **port conflicts** via args; avoid hardcode ports.
- `sbt clean test` green; small smoke script included in PR description


## Key Decisions Made

- **IDL**: Week 4 front-half finalized (additive + backward compatible).
- **Architecture**: Master (gRPC server) ↔ Workers (gRPC clients + WorkerService callback server).
- **Splitters: Unsigned 10-byte quantiles**; partition ranges are `[lo, hi)`.
- **Testing strategy**: Scale from small smoke to medium (10–100 MB) and beyond.

## Technical Notes

### Protobuf Service Design (Current)

```protobuf
service MasterService {
  rpc RegisterWorker(WorkerInfo) returns (RegistrationResponse);
  rpc Heartbeat(WorkerInfo) returns (Ack);
  rpc SendSamples(stream Sample) returns (Splitters);
}

service WorkerService {
  rpc SetPartitionPlan(PartitionPlan) returns (Ack);
  rpc PushPartition(stream PartitionChunk) returns (Ack); // for future shuffle
}
```

### Record Reading Strategy

```
val recordSize = 100
val buf = new Array[Byte](recordSize)
while (input.read(buf) == recordSize) {
  val key   = java.util.Arrays.copyOfRange(buf, 0, 10)
  val value = java.util.Arrays.copyOfRange(buf, 10, 100)
  // sampling / bucketing / writing ...
}
```
