# Week 6 Progress (Nov 17 - Nov 23, 2025)

## This Week's Progress

### What We Completed

#### 1. Progress Presentation & Feedback

- Delivered progress presentation to course staff
- **Feedback received**: Question about dual main functions in WorkerClient and WorkerServer
  - Clarification: WorkerServer's standalone main is kept for testing purposes only
  - In actual system operation, WorkerServer is instantiated by WorkerClient

#### 2. Cluster Deployment & Testing

- **Deploy Script (`deploy.sh`)**
  - Created comprehensive deployment script for cluster management
  - Supports commands: `init`, `update`, `gensort`, `gendata`, `clean`, `reset`, `start`, `all`
  - Dynamic worker count configuration (1-20 workers)
  - Automated data generation with proper key range distribution

- **Master Port Assignment**
  - Implemented Master-side port allocation (6000 + workerId)
  - Fixed port collision issues in localhost testing
  - Updated `WorkerRegistry` to use `info.withPort(assignedPort)`

- **End-to-End Cluster Testing**
  - Successfully tested on actual cluster environment (vm01 ~ vm03)
  - Verified Worker registration with correct IPs
  - Confirmed Shuffle communication between Workers using actual IP addresses
  - Validated Merge and Finalize phases complete correctly

- **Worker Lifecycle Fix**
  - Added `awaitFinalizeComplete()` to prevent premature Worker termination
  - Workers now wait for Master's finalize command before shutdown
  - All output partitions correctly written to disk

#### 3. Code Improvements

- Fixed `Sampling.scala` to accept all file types (removed `.dat` filter)
- Updated CLI parser to match assignment spec (removed `--id`, `--port` flags)
- Added `signalFinalizeComplete()` in WorkerServer's finalizePartitions handler

---

## Challenges/Issues

### Technical Challenges

- **Port 0 in Registry**: Protobuf case class requires `.withPort()` instead of `.copy()`
- **File path expansion**: `~` not recognized by Java - must use absolute paths
- **Permission issues**: `/data` directory requires sudo; switched to `~/data`

### Resolved Issues

- Sampling returning 0 samples due to file extension filter
- Workers terminating before receiving finalize command
- All Workers registering with same port in localhost testing

---

## Next Week's Goals (Week 7) — Fault Tolerance

### Team Goals – Milestone #4

Implement fault tolerance so that the system completes sorting even when a Worker is killed during execution.

### Strategy: Surviving Worker Takeover

When a Worker dies, remaining Workers take over its assigned partitions rather than restarting the failed Worker.

---

### Individual Responsibilities

#### Youngseo - Detection & Coordination

**Goal**: Master detects failures quickly and coordinates recovery

- **Worker State Machine**
  - States: `REGISTERED` → `SAMPLING` → `PARTITIONING` → `SHUFFLING` → `MERGING` → `COMPLETED` / `DEAD`
  - Track current phase per worker
  - Log state transitions

- **Fast Failure Detection**
  - Reduce heartbeat timeout (30s → 5-10s)
  - Immediate detection when Worker stops responding
  - Mark Worker as `DEAD` and log failure point

- **Partition Reassignment**
  - Determine which partitions the dead Worker was responsible for
  - Reassign to surviving Workers (round-robin or load-based)
  - Broadcast reassignment to affected Workers

- **Recovery Coordination RPC**
  - New RPC: `NotifyWorkerFailure(failed_worker_id, reassignments)`
  - Surviving Workers receive new partition assignments

**Deliverables**:
- Worker state tracking in Master
- Sub-10-second failure detection
- Partition reassignment logic
- Recovery notification to surviving Workers

---

#### Jimin - Data Resilience & Shuffle Recovery

**Goal**: Ensure Shuffle data survives Worker failure and can be recovered

- **Shuffle Checkpointing**
  - Save received partition data to disk immediately (not just in memory)
  - Track which partitions have been fully received
  - Maintain `ReceivedPartitions` manifest per Worker

- **Shuffle Retry Mechanism**
  - Detect when target Worker is dead during send
  - Queue unsent data for retry to reassigned Worker
  - Implement send timeout and retry with backoff

- **Partition Data Recovery**
  - On receiving reassignment, load checkpoint data
  - Request missing partitions from other Workers
  - New RPC: `RequestPartitionData(partition_id)` for recovery

- **Duplicate Handling**
  - Idempotent partition receive (ignore duplicates)
  - Sequence number validation

**Deliverables**:
- Persistent partition storage during Shuffle
- Retry logic for failed sends
- Recovery protocol for missing partitions
- Duplicate detection

---

#### Sangwon - Recovery Completion & Verification

**Goal**: Ensure system completes correctly after recovery

- **Recovery Completion Logic**
  - Track which Workers have completed recovery
  - Adjust completion conditions (N-1 Workers if one died)
  - Handle multiple failure scenarios

- **Output Integrity Verification**
  - Verify all partitions are present after recovery
  - Check record counts match expected totals
  - Validate global sort order across all outputs

- **Cleanup & Finalization**
  - Clean up partial data from dead Worker
  - Ensure no duplicate partitions in final output
  - Final merge handles recovered partitions correctly

- **Testing & Scenarios**
  - Test failure at each phase (Sampling, Shuffle, Merge)
  - Verify correct output with `valsort`
  - Document recovery behavior

**Deliverables**:
- Adjusted completion tracking
- Integrity verification tools
- Comprehensive fault tolerance tests
- Recovery documentation

---

### Implementation Timeline

#### Phase 1: Foundation
- **Youngseo**: Worker state machine + fast detection
- **Jimin**: Shuffle checkpoint design + implementation
- **Sangwon**: Recovery scenarios documentation + test plan

#### Phase 2: Core Recovery
- **Youngseo**: Partition reassignment + notification RPC
- **Jimin**: Shuffle retry + partition recovery RPC
- **Sangwon**: Completion tracking + integrity checks

#### Phase 3: Integration & Testing
- All: Integration testing with Worker kills
- All: Fix edge cases and verify with `valsort`

---

### Cross-Team Interfaces

#### New Proto Definitions

```protobuf
// Worker state reporting
message WorkerStateUpdate {
  int32 worker_id = 1;
  string phase = 2;  // "sampling", "shuffling", "merging", etc.
}

// Failure notification
message WorkerFailureNotification {
  int32 failed_worker_id = 1;
  repeated PartitionReassignment reassignments = 2;
}

message PartitionReassignment {
  int32 partition_id = 1;
  int32 new_owner_id = 2;
}

// Partition recovery
message PartitionDataRequest {
  int32 partition_id = 1;
  int32 requester_id = 2;
}
```

#### Dependency Chain

```
Youngseo (Detection) → Jimin (Data Recovery) → Sangwon (Completion)
     ↓                      ↓                        ↓
 State Machine        Checkpoint/Retry         Verification
 Fast Detection       Recovery RPC             Final Output
 Reassignment         Duplicate Handle         Testing
```

---

### PR Checklist

- [ ] Unit tests for failure scenarios
- [ ] Logs include worker state and recovery actions
- [ ] Checkpoint files properly managed (create/cleanup)
- [ ] Recovery tested at each phase
- [ ] `valsort` passes after recovery
- [ ] Documentation updated with fault tolerance behavior

---

## Key Decisions Made

- **Recovery Strategy**: Surviving Workers take over (no Worker restart)
- **Checkpoint Scope**: Shuffle data persisted to disk
- **Detection Speed**: Target sub-10-second failure detection
- **Reassignment**: Master coordinates, Workers execute

---

## Technical Notes

### Failure Detection Flow

```
1. Worker stops sending heartbeat
2. Master detects timeout (5-10s)
3. Master marks Worker as DEAD
4. Master calculates partition reassignment
5. Master broadcasts failure notification
6. Surviving Workers:
   - Update their partition assignments
   - Request missing data if needed
   - Continue processing
7. Master adjusts completion criteria
```

### Checkpoint Strategy

```
Shuffle Phase:
- Sender: Keep partition data until ACK received
- Receiver: Write to disk immediately, update manifest

Recovery:
- New owner reads checkpoint manifest
- Requests missing partitions from peers
- Continues merge with recovered data
```

---

## Resources

- Current codebase with cluster testing complete
- Deploy script for easy testing (`deploy.sh`)
- 20 Worker VMs available (vm01-vm20)
