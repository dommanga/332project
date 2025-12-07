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

### **4. Shuffle Checkpointing (Sender-Side)**

During shuffle, each worker checkpoints every partition *before* sending it out.

For each partition `pid`:

```
<output_dir>/sent-checkpoint/sent_p<pid>.dat
```

These files contain the exact bytes that were streamed to other workers.
If a worker crashes mid-shuffle, these files remain intact.

On restart, the worker checks:

```scala
hasSentCheckpoints(outputDir)
```

If `true`, the worker enters **Recovery Mode**, meaning:

* Skip sampling / local sort / partitioning / shuffle
* Immediately report shuffle completion to Master
* Later serve these same checkpointed partitions to other workers during finalize

This guarantees the worker never recomputes shuffle output after a crash.

### **5. Receiver-Side Checkpointing**

Whenever a worker *receives* a partition from another worker, it writes it as a run file:

```
<output_dir>/recv/p<pid>_from_w<source>.dat
```

These runs capture *all data the worker collected* during shuffle.
If a worker is restarted, these run files still exist and are used during finalize.

During finalize, each worker:

1. Checks which partitions it already has runs for
2. Requests missing runs from the responsible workers
3. Performs a **disk-based k-way merge** to write final outputs like:

```
<output_dir>/partition.<pid>
```

This ensures a worker can finish finalize deterministically, even if shuffle was interrupted.

### **6. Recovery Mode (Worker Restart)**

A worker restarted on the same node with the same CLI args behaves as a continuation of the previous execution.

At startup:

```
🔄 Recovery mode: waiting for finalize...
```

The worker:

1. Restores its previous server port from `.worker_state*`
2. Re-registers with Master using the same worker ID
3. Reports shuffle completion using sender-side checkpoints
4. Waits for Master to re-send the PartitionPlan (needed after crash)
5. Runs finalize:

   * Serves its checkpointed partitions to others
   * Requests missing partitions from peers
   * Performs disk-based merge to regenerate its final outputs

Example log (actual):

```
🔍 Checking 4 partitions for missing data...
🔄 Requesting p8 from w0...
🔄 Requesting p8 from w1...
🔄 Requesting p8 from w2...
📤 Sending p8 to w2
🔄 Regenerating p8...
```

When finalize completes:

```
✅ Merge completion reported to Master
```

After Master broadcasts shutdown:

```
🧹 Cleaning worker state dir: .worker_state__home_orange_out
💀 Worker shutting down...
```

This model ensures:

* A crashed worker can **always** recover
* Data sent before the crash is **never recomputed**
* Data received before the crash is **never lost**
* The worker's ID, port, and partition responsibilities remain consistent

---

## Implementation Notes

### Master CLI

```bash
java -jar target/scala-2.13/dist-sort.jar master <num_workers>
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
java -jar target/scala-2.13/dist-sort.jar worker <master_ip:port> -I <input_dir> -O <output_dir>
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
FAULT_INJECT_PHASE=mid-shuffle FAULT_INJECT_WORKER=1 java -jar target/scala-2.13/dist-sort.jar worker 2.2.2.254:38278 -I /dataset/small -O /home/orange/out
```

Then restart the same command **without** fault injection to recover.

---

## Cluster Testing

### Environment

* **Master**: `vm-1-master` (e.g., `2.2.2.254`)
* **Workers**: `vm01` ~ `vm20` (e.g., `2.2.2.101` ~ `2.2.2.120`)
* **Dataset**: Provided under `/dataset`

### 1. Preparation

On master node:
* Master and Each worker should have dist-sort.jar already
  * If not, use deploy.sh (Cluster Helper Script - Refer to below section)

```bash
cd ~/332project
```

### 2. Start Master

```bash
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

On three worker VMs (e.g., `vm17`, `vm18`, `vm19`):

```bash
# Worker 0
java -jar target/scala-2.13/dist-sort.jar worker 2.2.2.254:38278 -I /dataset/small -O /home/orange/out

# Worker 1
java -jar target/scala-2.13/dist-sort.jar worker 2.2.2.254:38278 -I /dataset/small -O /home/orange/out

# Worker 2 (with fault injection example)
FAULT_INJECT_PHASE=mid-shuffle FAULT_INJECT_WORKER=2 java -jar target/scala-2.13/dist-sort.jar worker 2.2.2.254:38278 -I /dataset/small -O /home/orange/out
```

After the crash, restart Worker 2 without fault injection:

```bash
java -jar target/scala-2.13/dist-sort.jar worker 2.2.2.254:38278 -I /dataset/small -O /home/orange/out
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

DEFAULT_NUM_WORKERS=3
```

### Commands

### Commands

| Command    | Description                                                                |
|-----------|-----------------------------------------------------------------------------|
| `init`    | Initial setup on workers (git clone `332project`, create input/output dirs) |
| `update`  | `git pull origin main` on all selected workers                              |
| `jar`     | Deploy `dist-sort.jar` to all selected workers                              |
| `gensort` | Copy `gensort` and `valsort` binaries from `PROJECT_DIR` to each worker     |
| `gendata` | Generate test input data on workers using `gensort`                         |
| `clean`   | Remove all files in `DATA_OUTPUT` on workers                                |
| `reset`   | Currently equivalent to `clean` (can be extended to `clean+gendata`)        |
| `start`   | Start worker processes via `java -jar target/scala-2.13/dist-sort.jar worker ...` (requires `num_workers` and `MASTER_PORT`) |
| `restart` | Restart a **single** worker using the same jar-based `worker` command                            |
| `stop`    | Kill all `worker.WorkerClient` processes on the selected workers            |
| `logs`    | Show last 50 lines of `/tmp/worker.log` from each worker                    |
| `check`   | Run `valsort` on each partition and compare global input/output record counts |
| `all`     | `update` + `reset`: prepare workers before starting a new experiment        |

### Typical Workflow

```bash
# Before the first test
sbt assembly
./deploy.sh jar

# Before each test
./deploy.sh reset

# Terminal 1: Master (manual)
java -Xms1G -Xmx2G -XX:MaxDirectMemorySize=4G -jar target/scala-2.13/dist-sort.jar master 3
# Master prints something like:
#    2.2.2.254:45729
#    2.2.2.117, 2.2.2.118 2.2.2.119
# Use this PORT for deploy.sh

# Normal run (no fault injection)
./deploy.sh start 3 45729

# FT run: crash worker 2 at mid-shuffle
FAULT_INJECT_PHASE=after-partition FAULT_INJECT_WORKER=2 ./deploy.sh start 3 45729

# FT run: Suppose Worker 2 was running on vm19 (refer to ordering of workers)
./deploy.sh restart vm19 45729

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