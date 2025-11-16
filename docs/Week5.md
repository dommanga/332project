# Week 5 Progress (Nov 10 - Nov 16, 2025)

## This Week's Progress

### 1. Worker-Side Local Sort, Partitioning & Shuffle Sender (Sangwon)

* Local record loading (100-byte records) and 10-byte unsigned key extraction
* Full local sort implemented using custom comparator `RecordIO.compareKeys`
* Splitter-based partitioning using `[lo, hi)` semantics
* Worker→Worker shuffle sender implemented using gRPC streaming (`PushPartition`)
* PartitionChunk format applied with sequential seq numbers
* Temporary routing rule (pid % numWorkers) pending full PartitionPlan integration
* sbt compile passed, local run verified

### 2. WorkerServer Shuffle Receiver Stub (Youngseo)

* `PushPartition` server-side implemented
* Receives streamed chunks; tracks partition id, chunk count, last sequence number
* Ready for Week6 merge integration

### 3. Master-Side Updates (Jimin)

* Integrated with Week4 sample→splitter pipeline
* PartitionPlan broadcast logic validated
* WorkerRegistry liveness tracking maintained

## Challenges / Issues

* Missing Ordering for Array[Byte] resolved via custom comparator
* Splitter→worker routing not yet integrated into WorkerClient
* Backpressure + checksum + retry logic scheduled for Week6

## Next Week’s Goals (Week 6)

### Worker Receiver & Merge (Youngseo)

* Persist incoming chunks
* Build per-partition merge pipeline
* Global k-way merge + final sorted output

### Shuffle Reliability (Jimin)

* Retry/backoff for RPC failures
* Add traceId for debugging

### Integration Test (All)

* Full pipeline: register → heartbeat → samples → splitters → plan → sort → shuffle → merge → output

## PR Checklist

* [x] Shuffle sender implemented
* [x] Comparator integrated
* [x] Partitioning logic complete
* [ ] Merge pipeline pending
* [ ] Full integration test pending

## Key Decisions

* Unsigned lexicographic key ordering
* PartitionChunk seq-level ordering for retries
* Temporary routing rule until full PartitionPlan integration

## Technical Notes

### PartitionChunk

```
message PartitionChunk {
  TaskId task = 1;
  string partition_id = 2;
  bytes payload = 3;  // multiple of 100 bytes
  int64 seq = 4;
}
```

### Worker→Worker Streaming Snippet

```
val stub = WorkerServiceGrpc.stub(channel)
val req = stub.pushPartition(responseObserver)

var seq = 0L
for (rec <- records) {
  req.onNext(
    PartitionChunk(
      task = Some(TaskId("task-001")),
      partitionId = s"p$pid",
      payload = ByteString.copyFrom(rec),
      seq = seq
    )
  )
  seq += 1
}
req.onCompleted()
```
