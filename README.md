# 332 Distributed Sorting Project

## Team Information

- **Members**: Jimin, Sangwon, Youngseo
- **Course**: CSE332 Software Design Methods (Fall 2025)

---

## Project Overview

Fault-tolerant distributed sorting system for key/value records across multiple machines.

### Requirements

* **Record format**: 100-byte record

  * 10-byte key
  * 90-byte value
* **Architecture**: 1 Master, N Workers
* **Goal**:

  * Globally sorted output over all workers
  * Handles **worker crashes during execution** (mid-shuffle, mid-finalize, etc.)

### High-Level Flow

```text
1. Sampling
   - Workers send sampled keys to Master.
   - Master chooses global splitters.

2. Local Sort & Partition
   - Each worker loads its local input.
   - Parallel local sort using multiple threads.
   - Partition into key ranges according to splitters.

3. Shuffle
   - Workers send partitions to target workers using gRPC streaming.
   - Sender-side checkpoint for sent partitions.

4. Final Merge & Output
   - Each worker merges received partitions.
   - Writes out final sorted partitions.
   - Reports completion to Master.
```

---

## Fault Tolerance Design

### Worker Failure Model

Scenario from spec:

> A worker process crashes in the middle of execution.
> A new worker starts on the same node (using the same parameters).
> The new worker should generate the same output expected of the initial worker.

Our implementation supports:

* Crash **after-sampling**, **after-sort**, **after-partition**, **mid-shuffle**, **before-finalize**.
* Restarted worker uses the **same worker ID** and (logically) the same role in the system.

### Components

#### 1. Heartbeat + Worker Registry (Master)

* Master tracks each worker in `WorkerRegistry`:

  * `WorkerPhase = ALIVE | DEAD`
  * Keyed by worker **IP** (`2.2.2.xxx`).
  * Each worker gets a **stable ID** (`0 .. N-1`) when first registered.

* Background thread on Master:

  ```scala
  registry.pruneDeadWorkers(timeoutSeconds = 5) { deadId =>
    handleWorkerFailure(deadId)
  }
  ```

* If a worker misses heartbeats for more than `timeoutSeconds`:

  * Marked as **DEAD**.
  * All partitions owned by that worker are marked **orphaned**.
  * Master prints:

    ```text
    💀 Worker 1 DEAD (no heartbeat for 7s)
    ⚠️  Worker 1 failed - partitions Set(5, 6, 7, 4) orphaned
    ℹ️  Please restart Worker 1 to recover
    ```

#### 2. Worker Re-Join (Same Node, Same ID)

* When a new worker process starts **on the same node** with the same `worker.WorkerClient` command:

  * Worker calls `registerWorker` with its IP and the same port as before (via local port state).
  * `WorkerRegistry.register`:

    * If there is a dead worker with the same IP:

      * Reuses the **same worker ID**.
      * Marks phase = `ALIVE`.
      * Prints:

        ```text
        🔄 Worker 2.2.2.118 rejoining with ID 1
        🎉 Worker 1 rejoined!
        ```

* Master updates `partitionOwners` for orphaned partitions to point to the rejoined worker.

#### 3. Partition Plan & Re-broadcast

* Master caches the latest `PartitionPlan` in `PlanStore`.

* On sampling completion, Master:

  * Computes splitters.
  * Creates `PartitionPlan` with worker addresses.
  * Broadcasts:

    ```text
    📋 Broadcasting PartitionPlan to workers...
    ✅ Shuffle phase started
    ```

* When a worker re-joins:

  * Master reassigns orphaned partitions.
  * Sends the same logical PartitionPlan only to the rejoined worker, because all other workers already have the correct plan:

    ```text
    ✅ Resent PartitionPlan to Worker 1
    ```

#### 4. Sender-Side Checkpoint (Worker)

* Each worker, during shuffle, **checkpoints the data it sends**:

  * For each partition `pid`, before sending:

    ```scala
    checkpointSentPartition(pid, recs, outputDir)
    ```

  * Stored under:

    ```text
    <outputDir>/sent-checkpoint/sent_p<PID>.dat
    ```

* On restart, worker checks:

  ```scala
  hasSentCheckpoints(outputDir)
  ```

  * If `true` → **recovery mode**:

    * Skip sampling / local sort / partition / shuffle.
    * Wait for finalize command from Master.
    * Reconstruct missing partitions using checkpoint & other workers.

#### 5. Recovery Mode (Worker)

On restart (same VM, same CLI args):

1. Worker starts `WorkerServer` (with port persistence).

2. Registers with Master → gets same worker ID.

3. Detects existing `sent-checkpoint` → prints:

   ```text
   🔄 Recovery mode: waiting for finalize...
   ```

4. After Master sees all shuffles complete (including rejoined worker), it calls `finalizePartitions` on all workers.

5. Worker:

   * Checks which partitions are missing.
   * Requests those partitions from available workers.
   * Prints logs like:

     ```text
     ⚠️  p4 missing from workers: Set(0, 1)
     🔄 Requesting p4 from w0...
     ✅ Received p4 from w0: 77178 records
     ```

6. Writes final partitions:

   ```text
   🔧 Finalizing 4 partitions...
   ✅ Wrote partition.4
   ✅ Wrote partition.5
   ✅ Wrote partition.6
   ✅ Wrote partition.7
   ```

7. Reports merge completion to Master → normal shutdown.

---

## Implementation Notes

### Master CLI

```bash
sbt assembly

java -Xms1G -Xmx2G -XX:MaxDirectMemorySize=4G -jar target/scala-2.13/dist-sort.jar master <num_workers>
```

* Master binds to port `0` (OS chooses a free port).

* On startup it prints:

  ```text
  <MASTER_IP>:<PORT>
  <ordering of IP addresses of workers>

  📌 All workers registered!

  📋 Broadcasting PartitionPlan to workers...
  ```

* You must pass this `<MASTER_IP>:<PORT>` to the workers.

### Worker CLI

```bash
java -Xms2G -Xmx4G -XX:MaxDirectMemorySize=8G -XX:+UseG1GC -XX:MaxGCPauseMillis=200 -jar target/scala-2.13/dist-sort.jar worker <master_ip:port> -I <input_dir> -O <output_dir>
```

* Worker:

  * Starts its own gRPC server on a dynamically chosen port.
  * Registers itself at Master with its IP + port.
  * Runs sampling → local sort → partition → shuffle → finalize.

### Fault Injection

We use env vars for deterministic failure testing:

* `FAULT_INJECT_PHASE` (comma-separated phases):

  * `after-sampling`
  * `after-sort`
  * `after-partition`
  * `mid-shuffle`
  * `before-finalize`

* `FAULT_INJECT_WORKER`:

  * `n` = apply only to worker with ID `n`

Example (crash worker 1 mid-shuffle):

```bash
FAULT_INJECT_PHASE=mid-shuffle FAULT_INJECT_WORKER=1 java -Xms2G -Xmx4G -XX:MaxDirectMemorySize=8G -XX:+UseG1GC -XX:MaxGCPauseMillis=200 -jar target/scala-2.13/dist-sort.jar worker 2.2.2.254:38278 -I /dataset/small -O /home/orange/out
```

Then restart the same command **without** fault injection to recover.

---

## Quick Start (Local)

### Prerequisites

* Java 8+
* Scala 2.13
* SBT 1.x
* (Optional) `gensort` + `valsort` for synthetic data

### Build

```bash
sbt compile
```

### Generate Local Test Data (Optional)

```bash
wget http://www.ordinal.com/try.cgi/gensort-linux-1.5.tar.gz
tar -xzf gensort-linux-1.5.tar.gz

mkdir -p data/input1 data/input2 data/input3

./gensort -a -b0      100000 data/input1/data
./gensort -a -b100000 100000 data/input2/data
./gensort -a -b200000 100000 data/input3/data
```

---

## Cluster Testing

### Environment

* **Master**: `vm-1-master` (e.g., `2.2.2.254`)
* **Workers**: `vm01` ~ `vm20` (e.g., `2.2.2.101` ~ `2.2.2.120`)
* **Dataset**: Provided under `/dataset`

### 1. Deploy / Update Code

On each worker VM:

```bash
cd ~/332project
git pull origin main
sbt compile
```

### 2. Start Master

On `vm-1-master`:

```bash
cd ~/332project
sbt assembly

java -Xms1G -Xmx2G -XX:MaxDirectMemorySize=4G -jar target/scala-2.13/dist-sort.jar master 3
```

Example output:

```text
2.2.2.254:38278
2.2.2.117, 2.2.2.118 2.2.2.119

📌 All workers registered!

📋 Broadcasting PartitionPlan to workers...
✅ Shuffle phase started
```

Use the printed `2.2.2.254:38278` as `<master_ip:port>` for workers.

### 3. Start Workers

On two worker VMs (e.g., `vm17`, `vm18`, `vm19`):

```bash
# Worker 0
java -Xms2G -Xmx4G -XX:MaxDirectMemorySize=8G -XX:+UseG1GC -XX:MaxGCPauseMillis=200 -jar target/scala-2.13/dist-sort.jar worker 2.2.2.254:38278 -I /dataset/small -O /home/orange/out

# Worker 1
java -Xms2G -Xmx4G -XX:MaxDirectMemorySize=8G -XX:+UseG1GC -XX:MaxGCPauseMillis=200 -jar target/scala-2.13/dist-sort.jar worker 2.2.2.254:38278 -I /dataset/small -O /home/orange/out

# Worker 2 (with fault injection example)
FAULT_INJECT_PHASE=mid-shuffle FAULT_INJECT_WORKER=2 java -Xms2G -Xmx4G -XX:MaxDirectMemorySize=8G -XX:+UseG1GC -XX:MaxGCPauseMillis=200 -jar target/scala-2.13/dist-sort.jar worker 2.2.2.254:38278 -I /dataset/small -O /home/orange/out
```

After the crash, restart Worker 2 without fault injection:

```bash
java -Xms2G -Xmx4G -XX:MaxDirectMemorySize=8G -XX:+UseG1GC -XX:MaxGCPauseMillis=200 -jar target/scala-2.13/dist-sort.jar worker 2.2.2.254:38278 -I /dataset/small -O /home/orange/out
```

You should see:

* On Master:

  ```text
  💀 Worker 2 DEAD (no heartbeat for 7s)
  ⚠️  Worker 2 failed - partitions Set(8, 9, 11, 10) orphaned
  ℹ️  Please restart Worker 2 to recover
  🔄 Worker 2.2.2.119 rejoining with ID 2
  🎉 Worker 2 rejoined!
  📦 Assigning recovery partitions: Set(8, 9, 11, 10)
  ✅ Resent PartitionPlan to Worker 2
  ...
  🎉 Distributed sorting complete!
  ```

* On restarted Worker:

  ```text
  🔄 Recovery mode: waiting for finalize...
  🔧 Starting finalize phase...
  ...
  ✅ Worker work completed
  💀 Worker shutting down...
  ```

---

## `deploy.sh` (Cluster Helper Script)

We provide a helper script for common cluster tasks.

### Key Configuration (inside `deploy.sh`)

```bash
PROJECT_DIR="/home/orange/332project"

DATASET="small"
DATA_INPUT="/dataset/${DATASET}"
DATA_OUTPUT="/home/orange/out"

MASTER_IP="2.2.2.254"
RECORDS_PER_WORKER=100000

DEFAULT_NUM_WORKERS=3
```

### Commands

### Commands

| Command    | Description                                                                 |
|-----------|-----------------------------------------------------------------------------|
| `init`    | Initial setup on workers (git clone `332project`, create input/output dirs) |
| `update`  | `git pull origin main` + `sbt compile` on all selected workers              |
| `gensort` | Copy `gensort` and `valsort` binaries from `PROJECT_DIR` to each worker     |
| `gendata` | Generate test input data on workers using `gensort`                         |
| `clean`   | Remove all files in `DATA_OUTPUT` on workers                                |
| `reset`   | Currently equivalent to `clean` (can be extended to `clean+gendata`)       |
| `start`   | Start worker processes via `java -jar target/scala-2.13/dist-sort.jar worker ...` (requires `num_workers` and `MASTER_PORT`) |
| `restart` | Restart a **single** worker using the same jar-based `worker` command                            |
| `stop`    | Kill all `worker.WorkerClient` processes on the selected workers            |
| `logs`    | Show last 50 lines of `/tmp/worker.log` from each worker                    |
| `check`   | Run `valsort` on each partition and compare global input/output record counts |
| `all`     | `update` + `reset`: prepare workers before starting a new experiment        |

### Typical Workflow

```bash
# One-time setup
./deploy.sh init
./deploy.sh gensort   # if using gensort-based data

# Before each test
./deploy.sh update
./deploy.sh reset

# Terminal 1: Master (manual)
java -Xms1G -Xmx2G -XX:MaxDirectMemorySize=4G -jar target/scala-2.13/dist-sort.jar master 3
# Master prints something like:
#   2.2.2.254:45729
# Use this PORT for deploy.sh

# Normal run (no fault injection)
./deploy.sh start 3 45729

# FT run: crash worker 2 at mid-shuffle
FAULT_INJECT_PHASE=mid-shuffle FAULT_INJECT_WORKER=2 ./deploy.sh start 3 45729

# FT run: Suppose Worker 2 was running on vm17
./deploy.sh restart vm17 45729

# Run valsort on all partitions and compare global input/output records
./deploy.sh check 3

# Tail worker logs
./deploy.sh logs 3

# Kill all worker clients if needed
./deploy.sh stop 3
```

---

## Architecture Summary

### Master

* Manages:

  * Worker registration (`WorkerRegistry`)
  * Heartbeats + failure detection
  * Sampling + splitter computation
  * `PartitionPlan` creation & broadcasting
  * Partition ownership (`partitionOwners`)
  * Shuffle & merge progress (`ShuffleTracker`)
  * Shutdown broadcast when all merges are done

### Worker

* Performs:

  * Input load (from `-I` paths)
  * Parallel local sorting (multi-thread)
  * Partitioning by key ranges
  * Shuffle via gRPC streaming (with retry + checkpoint)
  * Recovery using `sent-checkpoint` data
  * Final merge + output
  * Periodic heartbeat to Master

---

## Technology Stack

* **Language**: Scala 2.13
* **Build Tool**: SBT
* **RPC Framework**: gRPC via ScalaPB
* **Data Generator**: `gensort` (optional, for local tests)
* **Cluster Environment**: POSTECH VMs (`2.2.2.xxx`)
* **Scala**: 2.13.13, **sbt**: 1.11.7, **Java**: 1.8

---

## Repository Structure

```text
332project/
├── src/
│   ├── main/
│   │   ├── scala/
│   │   │   ├── master/        # MasterServer, MasterServiceImpl, WorkerRegistry, etc.
│   │   │   ├── worker/        # WorkerClient, WorkerState, WorkerServer, etc.
│   │   │   └── common/        # RecordIO, sampling helpers
│   │   └── protobuf/          # gRPC proto files
│   └── test/                  # (Optional) tests
├── docs/                      # Weekly progress, design notes
├── deploy.sh                  # Cluster deployment helper
├── build.sbt
└── README.md
```