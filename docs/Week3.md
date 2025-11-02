# Week 3 Progress (Oct 27 - Nov 2, 2025)

## This Week's Progress

### What We Completed

#### 1. Protobuf / Interface (Youngseo)

- Extended `sort.proto` to cover Core-Sorting front-half
  - **MasterService**: `RegisterWorker`, `Heartbeat`, `SendSamples(stream Sample) -> Splitters`
  - **WorkerService**: `SetPartitionPlan(PartitionPlan) -> Ack`, `PushPartition(stream PartitionChunk) -> Ack` (shuffle prep)
- Added/updated messages
  - `Ack`, `WorkerInfo{host,port}`, `RegistrationResponse{worker_index}`
  - `Sample`, `Splitters`, `PartitionRange`, `PartitionPlan`
  - `PartitionChunk`, `StreamDone`, `TaskId`, `BlockMeta`
- Comments unified in English; additive-only change policy documented
- Opened PR `feat/proto`; local `sbt clean/compile/test` passed

#### 2. Build & Project Scaffold (tedoh7)

- Initialized SBT + ScalaPB/gRPC toolchain
  - `build.sbt`, `project/build.properties`, `project/plugins.sbt`
- Seeded initial files
  - `.gitignore`, initial `sort.proto`, mock runMain entries
  - `master.MasterServer`, `worker.WorkerClient` (mock logs)
- Verified test runner with `ExampleSpec.scala`
- Implemented `common/RecordIO.scala`
  - 100-byte record streaming I/O
  - 10-byte **unsigned** lexicographic key compare

#### 3. Master & Worker Implementation (Jimi)

- WorkerClient gRPC implementation
- Command-line argument parsing (-I, -O)
- Master registration via gRPC

- Test
  Master + 2 Workers successfully connected:

  Master: 172.30.1.51:5000
  Worker 0: Partitions 0, 1, 2
  Worker 1: Partitions 3, 4, 5

#### 4. Documentation & Repo Hygiene

- Updated `README.md` with Week 3 milestone and unified run commands
- Added `docs/Week3.md`; organized doc locations

```
sbt -Dsbt.global.base="$HOME/.sbt-global-clean" compile
sbt -Dsbt.global.base="$HOME/.sbt-global-clean" "runMain master.MasterServer"
sbt -Dsbt.global.base="$HOME/.sbt-global-clean" "runMain worker.WorkerClient"
sbt -Dsbt.global.base="$HOME/.sbt-global-clean" test
```

## Challenges/Issues

### Technical Challenges

- Streaming pattern decisions to keep implementations simple
  - **Client-streaming** for `SendSamples`, **unary** reply with `Splitters`
  - **Streaming** reserved for future shuffle `PushPartition`
- JDK 21/24 `Unsafe` / native-access warnings under sbt — currently non-blocking

### Team Coordination

- Keeping interface stable enough for **parallel** Master/Worker development next week
- Aligning CLI flags and run flows across members

## Next Week's Goals (Week 4) — Core Sorting Logic

### Team Goals – Milestone #2

#### 1. Implement Real Master (Jimin)

- Bind gRPC server; print IP:port on startup
- Maintain in-memory **WorkerRegistry** (id/host/port/lastBeat; pruning)
- Handle `RegisterWorker`, periodic `Heartbeat`
- Receive `SendSamples` (client-stream) and buffer per task

#### 2. Implement Real Worker (Sangwon)

- CLI: `--master host:port -I <in> ... -O <out> --id wX --port <p>`
- Connect and register to master; send periodic heartbeats
- Read 100-byte records; extract uniform **samples**; stream via `SendSamples`

#### 3. Splitter & Plan (Youngseo)

- Aggregate all samples at master; **unsigned** sort; pick **k−1** quantile splitters
- Build `PartitionPlan` and broadcast via `WorkerService.SetPartitionPlan` (workers ACK)
- Prepare integration scenarios & minimal metrics/logging

#### 4. Integration Test

- Master + 2–3 Workers successfully connect
- Verify: register → heartbeat → sample upload → splitter reply → plan broadcast
- Validate logs: sample counts, number of splitters, plan delivery

### Individual Responsibilities

#### Jimin

- Master gRPC server, `WorkerRegistry`
- Handle Register/Heartbeat/SendSamples
- Document Master API and logs/metrics

#### Sangwon

- Worker gRPC client, CLI parsing
- Sampling + client-streaming via `SendSamples`
- Graceful shutdown/retry

#### Youngseo

- Splitter selection, `PartitionPlan` builder
- Proto docs & integration checklist
- Plan broadcast via `SetPartitionPlan`

## Key Decisions Made

- **IDL** finalized for Core Sorting front-half (additive, backward-compatible)
- **Architecture**: Master as gRPC server; Workers as gRPC clients (plus WorkerService server for callbacks)
- **Testing approach**: Start with 10–100 MB datasets; add smoke script after E2E stands

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

### Key Comparison Implementation

```
// 10-byte unsigned lexicographic compare
def compareKeys(a: Array[Byte], b: Array[Byte]): Int = {
  var i = 0
  while (i < 10) {
    val x = a(i) & 0xFF
    val y = b(i) & 0xFF
    if (x != y) return x - y
    i += 1
  }
  0
}
```

### Record Reading Strategy

```
// Read 100-byte records from binary file
val recordSize = 100
val buf = new Array[Byte](recordSize)
while (input.read(buf) == recordSize) {
  val key = buf.slice(0, 10)
  val value = buf.slice(10, 100)
  // Process record...
}
```
