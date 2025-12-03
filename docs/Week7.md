# Week 7 Progress (Nov 24 - Nov 30, 2025)

## This Week's Progress

### **1. Master-Side Fault Tolerance & Final Verification (Sangwon)**  
#### 🔥 Dead Worker Detection + Fault-Tolerant Shuffle/Merge  
- `WorkerRegistry.pruneDeadWorkers()`에 **callback 기반 dead-worker detection** 추가  
- `MasterServer` prune thread에서 **dead worker 발생 시 즉시**:
  - `ShuffleTracker.markWorkerFailed(deadId)` 호출
  - `handleMergeFailure(deadId)`로 orphaned partition 재배치 (Step2 구조 준비)
- `partitionOwners` 맵 기반 오너 추적 구조 도입 (향후 Step3에서 실제 merge 재할당에 활용 예정)

#### 🔥 Final Merge Completion → Verification 자동 실행  
- `reportMergeComplete()`에서 **모든 alive worker merge 완료 시** `triggerVerification()` 호출  
- 최종 검증 로직:
  - `worker.VerifyOutput.runFullVerification(outputDir, expectedPartitions, expectedRecords)` 호출
  - global record count, key ordering, partition continuity, overlap/gap 검사  
- 결과:
  - 성공 시: ✅ PASS 로그 출력  
  - 실패 시: ❌ FAIL 로그 출력  
- Master 기준 end-to-end 파이프라인 완성:  
  `register → heartbeat → samples → splitters → plan → shuffle → merge → verification`

#### 🔧 MasterServer 전체 코드 안정화  
- callback 기반 `pruneDeadWorkers` 적용해 dead worker를 Master에서 인지 가능하게 변경  
- `reportShuffleComplete` / `reportMergeComplete` 둘 다 누락 없이 구현  
- finalize phase + verification phase가 충돌하지 않도록 호출 순서 정리  
- 전체 `sbt compile` green 유지

---

### **2. Worker-Side Integration (Local Sort → Partition → Shuffle → Finalize) (Sangwon)**  
#### 🧩 WorkerClient.scala 통합 작업
- WorkerClient에서 수행 흐름 정리:
  1. Master에 등록 → `WorkerInfo` 전송, `WorkerAssignment` 수신  
  2. 샘플 전송 → `SendSamples` 스트리밍으로 sample key 전송  
  3. Splitters 수신 → `Splitters` 기준으로 key space 분할  
  4. Local sort:
     - 100-byte record에서 10-byte key 추출
     - `RecordIO.compareKeys` 기반 정렬
  5. Splitter 기반 partitioning:
     - `findPartition(extractKey(rec))`로 partition id 결정  
  6. WorkerServer가 저장한 `PartitionPlan`에서 **실제 Worker 주소 맵** 수신  
- Worker→Worker Shuffle:
  - `PartitionChunk(task, partitionId, payload, seq)` gRPC streaming 사용  
  - `pushPartition(responseObserver)` 형태로 송신  
  - 각 partition에 대해 target worker를 선택 후 전송
- Shuffle 완료 후:
  - `WorkerState.reportShuffleComplete()` 호출  
  - Master 쪽 finalize 명령 대기 (`WorkerState.awaitFinalizeComplete()`)  
  - 최종적으로 worker 로그에 “Worker completed successfully” 출력

#### 🔧 Heartbeat 추가 (버그 해결)
- Worker가 Master에 heartbeat를 보내지 않아 timeout으로 죽었다고 찍히던 문제 수정  
- `WorkerClient`에 `masterClient.heartbeat(info)` 호출 로직 재삽입  
- Master <-> Worker 간 liveness tracking이 의도대로 동작하도록 복구

---

### **3. Partition Planning & Routing Fix (Sangwon)**  
- `PartitionPlanner`:
  - fullMin / fullMax boundary 생성 시 `Array.fill` 사용 오류로 인한 컴파일 에러 수정  
  - 올바른 10-byte min/max 경계 생성:
    - `Array.fill(0x00.toByte)`
    - `Array.fill(0xFF.toByte)`
  - [lo, hi) semantics를 만족하는 range 리스트 생성 로직 유지
- `MasterServer`:
  - `PartitionPlanner.createPlan` 호출 시 `(Int, String, Int)` tuple 대신  
    `WorkerAddress` 목록을 그대로 넘기도록 수정  
  - Worker 주소 목록 구성:
    - `WorkerAddress(worker_id, ip, port)` 기반

---

### **4. WorkerRegistry Enhancement (Sangwon)**  
- `getAliveWorkers` 구현
  - 현재는 등록된 worker 전체를 alive로 간주 (추후 상태값 추가 시 확장 여지 확보)
- `pruneDeadWorkers(timeoutSeconds)(onDead: Int => Unit)` 형태의 **콜백 버전**으로 확장  
  - timeout 기준으로 죽은 worker를 찾고,
  - 각 worker id에 대해 `onDead(deadId)`를 호출하도록 변경  
- Master에서 dead worker 감지 후:
  - `ShuffleTracker.markWorkerFailed(deadId)`로 alive worker 수 재계산  
  - `handleMergeFailure(deadId)`에서 orphaned partition 재할당 로직 준비

---

### **5. VerifyOutput.scala 추가 (Sangwon + 팀 연동 지원)**  
- 출력 검증용 유틸리티 `worker.VerifyOutput` 구현:
  - record count 검증
  - global key ordering 검증
  - partition 간 key range overlap / gap 여부 검사  
- Master의 `triggerVerification()`에서 호출되도록 연결해  
  **end-to-end correctness**를 자동으로 확인할 수 있게 구성  
- 향후 팀원이 구현할 merge 로직/BlockMeta와 연동하기 위한 기반 마련

---

## Challenges / Issues

### ❗ 1. PartitionPlanner fullMin/fullMax 오류
- `Array.fill(0x00.toByte)` 형태 사용으로 인해 컴파일 실패  
→ `Array.fill(0x00.toByte)` / `Array.fill(0xFF.toByte)`로 수정

### ❗ 2. MasterServer pruneDeadWorkers 인수 mismatch
- 인자 없는 `pruneDeadWorkers()` 호출과  
  콜백 기반 `pruneDeadWorkers { deadId => ... }` 구현 사이 충돌  
→ WorkerRegistry를 콜백 버전으로 통일하고 Master 쪽 호출부 수정

### ❗ 3. reportShuffleComplete / reportMergeComplete 구현 누락
- "class MasterServiceImpl needs to be abstract" 컴파일 에러 발생  
→ 두 메서드를 다시 작성하여:
  - Shuffle/Merge 상태를 `ShuffleTracker`에 반영  
  - merge 완료 시 verification까지 자연스럽게 이어지는 흐름 구현

### ❗ 4. Heartbeat 호출 누락
- Worker가 Master에 heartbeat를 보내지 않아  
  일정 시간 이후 timeout으로 제거되는 문제  
→ WorkerClient에서 worker 시작 시 heartbeat 전송 로직을 다시 붙여넣어 해결

### ❗ 5. Finalize + Verification 순서 충돌 가능성
- finalize 이후 mergeComplete 수신 순서가 꼬일 수 있는 구조였음  
→ “모든 merge complete → verification → 종료” 구조로 재정렬하여  
  파이프라인 의미를 명확히 하고 디버깅을 쉽게 만듦

---

## Next Week’s Goals (Week 8)

### **1. True Fault-Tolerant Merge (Youngseo + Sangwon)**
- 현재는 orphaned partition 재할당 맵까지만 준비됨  
→ 실제 WorkerServer merge 코드와 연결해:
  - 죽은 worker가 담당하던 partition을 다른 worker가 대신 merge  
  - 중복 merge / 누락 없이 결과 보장

### **2. BlockMeta 기반 K-Way Merge 품질 향상 (Youngseo)**  
- `BlockMeta`(block_id, path, size, checksum …) 활용:
  - checksum 검증
  - block count / size mismatch 감지  
  - merge throughput, block 개수, 처리 시간 등 로깅

### **3. Full Scale Integration Test (All)**  
- 3-worker 시나리오로 통합 테스트:
  - 중간에 intentionally dead worker 만들어 failover 테스트  
  - VerifyOutput으로 최종 결과 검증
- Master / Worker 로그에:
  - 각 단계별 카운트, 실패/재시도 정보, merge/verification 통계 남기기

### **4. Logging/Tracing 강화 (Jimin 중심)**  
- `taskId` propagation 정리  
- WorkerState / Master 양쪽에 자세한 로그 라벨링  
- 장애 상황에서 어떤 partition/worker 조합에서 문제가 나는지 바로 보이도록 개선

---

## PR Checklist

- [x] Dead worker detection + callback prune
- [x] ShuffleTracker: alive worker 기반 카운트로 개선
- [x] `reportShuffleComplete` / `reportMergeComplete` 재구현
- [x] Master verification pipeline 완성 (`triggerVerification`)
- [x] Worker heartbeat 복구
- [x] PartitionPlanner boundaries fix
- [x] `VerifyOutput.scala` 통합
- [ ] True failover merge 구현 (Week 8)
- [ ] 3-worker 기준 stress test 및 장시간 테스트

---

## Key Decisions

- **Alive worker 기준**으로 Shuffle/Merge 완료 판정  
- Master prune thread를 통해 dead worker를 감지하고,  
  그 결과를 ShuffleTracker + handleMergeFailure로 넘기는 구조 채택  
- 최종 merge 완료 후 **자동 verification 실행** (수동 실행 필요 없음)  
- PartitionPlan에서 worker 주소는 `WorkerAddress` 리스트로 관리  
- Routing은 splitter 기반 partitioning을 기본으로 사용

---

## Technical Notes

### Master Failure Handling (Week 7)
```scala
registry.pruneDeadWorkers { deadId =>
  ShuffleTracker.markWorkerFailed(deadId)
  handleMergeFailure(deadId)
}
'''

### Verification Trigger
'''scala
override def reportMergeComplete(status: WorkerStatus): Future[Ack] = {
  Future {
    println(s"[Master] Worker ${status.workerId} reported merge complete")
    ShuffleTracker.markMergeComplete(status.workerId)

    if (ShuffleTracker.isAllMergeComplete) {
      println("[Master] All merge complete — running verification...")
      triggerVerification()
    }

    Ack(ok = true, msg = "Merge completion noted")
  }
}
'''

### Worker Shuffle Sender (WorkerClient)
'''scala
val stub = WorkerServiceGrpc.stub(channel)
val requestObserver = stub.pushPartition(responseObserver)

var seq: Long = 0
for (rec <- records) {
  val chunk = PartitionChunk(
    task        = Some(TaskId("task-001")),
    partitionId = s"p$partitionId",
    payload     = ByteString.copyFrom(rec),
    seq         = seq
  )
  seq += 1
  requestObserver.onNext(chunk)
}
requestObserver.onCompleted()
'''

### PartitionPlanner Boundaries
'''scala
val fullMin = Array.fill(0x00.toByte)
val fullMax = Array.fill(0xFF.toByte)

val bounds = Seq(fullMin) ++ splitters ++ Seq(fullMax)
'''
