# 332 Distributed Sorting Project

## Team Information

- **Members**: Jimin, Sangwon, Youngseo
- **Course**: CSE332 Software Design Methods (Fall 2025)

---

## Project Overview

Fault-tolerant distributed sorting system for key/value records across multiple machines.

### Requirements

- **Input**: 100-byte records (10-byte key + 90-byte value) distributed across workers
- **Output**: Sorted records distributed across workers with defined ordering
- **Architecture**: 1 Master + N Workers
- **Key Feature**: Must handle worker crashes during execution

### System Flow

```
1. Sampling:        Workers → Master (sample data)
2. Sort/Partition:  Workers sort locally and partition by key ranges
3. Shuffle:         Workers ↔ Workers (redistribute partitions)
4. Merge:           Workers merge and write final sorted output
```

---

## Quick Start

### Prerequisites

- Java 8+
- Scala 2.13
- SBT 1.x
- gensort (for test data generation)

### Build

```bash
sbt compile
```

---

## Testing Guide

### 1. Generate Test Data

First, generate test data using gensort:

```bash
# Download gensort (one-time setup)
wget http://www.ordinal.com/try.cgi/gensort-linux-1.5.tar.gz
tar -xzf gensort-linux-1.5.tar.gz

# Generate test data (100K records = 10MB)
mkdir -p data/input1 data/input2 data/input3
./gensort -a -b0 100000 data/input1/data
./gensort -a -b100000 100000 data/input2/data
./gensort -a -b200000 100000 data/input3/data

# Or use smaller data for quick testing (10K records = 1MB)
./gensort -a -b0 10000 data/input1/data
./gensort -a -b10000 10000 data/input2/data
./gensort -a -b20000 10000 data/input3/data
```

### 2. Local Testing (localhost)

Open 4 terminals and run in order:

**Terminal 1 - Master:**

```bash
sbt "runMain master.MasterServer 3"
```

Expected output:

```
Master Server Started
Address: 127.0.0.1:5000
Expected Workers: 3
```

**Terminal 2 - Worker 1:**

```bash
sbt "runMain worker.WorkerClient 127.0.0.1:5000 -I data/input1 -O out1"
```

**Terminal 3 - Worker 2:**

```bash
sbt "runMain worker.WorkerClient 127.0.0.1:5000 -I data/input2 -O out2"
```

**Terminal 4 - Worker 3:**

```bash
sbt "runMain worker.WorkerClient 127.0.0.1:5000 -I data/input3 -O out3"
```

**Verify results:**

```bash
ls out1/  # partition.0, partition.1, partition.2
ls out2/
ls out3/

# Validate sorting with valsort
./valsort out1/partition.0
./valsort out2/partition.1
./valsort out3/partition.2
```

### 3. Cluster Testing

#### Environment Information

- **Master**: 2.2.2.254 (or 141.223.16.227)
- **Workers**: 2.2.2.101 ~ 2.2.2.120 (vm01 ~ vm20)

#### Execution Steps

**Step 1: Deploy Code**

Deploy project code to all VMs (via git clone or scp):

```bash
# On each VM
git pull origin main
sbt compile
```

**Step 2: Start Master**

On the Master server (vm-1-master):

```bash
sbt "runMain master.MasterServer 3"
```

Expected output:

```
Master Server Started
Address: 2.2.2.254:5000
Expected Workers: 3
```

**Step 3: Start Workers**

On each Worker VM (via ssh):

```bash
# vm01 (2.2.2.101)
sbt "runMain worker.WorkerClient 2.2.2.254:5000 -I /data/input -O /data/output"

# vm02 (2.2.2.102)
sbt "runMain worker.WorkerClient 2.2.2.254:5000 -I /data/input -O /data/output"

# vm03 (2.2.2.103)
sbt "runMain worker.WorkerClient 2.2.2.254:5000 -I /data/input -O /data/output"
```

**Step 4: Verify Results**

Check output directory on each Worker:

```bash
ls /data/output/
# partition.0, partition.1, partition.2, ...
```

#### Cluster Testing Checklist

- [ ] Master logs show all Worker IPs registered correctly (2.2.2.101, etc.)
- [ ] PartitionPlan broadcasts with Worker addresses included
- [ ] Worker-to-Worker Shuffle uses actual IPs
- [ ] Partition files created on each Worker
- [ ] valsort validation passes

---

## Troubleshooting

### Common Issues

**1. "Connection refused" error**

- Ensure Master is running first
- Check firewall allows ports 5000, 6000
- Verify IP address is correct

**2. Worker IP shows as 127.0.0.1**

- `getLocalIP()` function not finding correct network interface
- Check actual IP with `hostname -I` command on VM

**3. Shuffle timeout**

- Target Worker not started yet
- Network connectivity issues

**4. "Unknown worker" error**

- PartitionPlan not received yet
- Worker registration order issue

### Log Checkpoints

**Master:**

```
Worker registered: id=0, ip=2.2.2.101:6000
Worker registered: id=1, ip=2.2.2.102:6000
All workers connected!
Broadcasting PartitionPlan to 3 workers...
```

**Worker:**

```
Received PartitionPlan for task=task-001
Received 3 worker addresses:
  worker#0 → 2.2.2.101:6000
  worker#1 → 2.2.2.102:6000
  worker#2 → 2.2.2.103:6000
```

---

## Architecture

### Master

- Coordinates all workers
- Calculates partition boundaries from samples
- Distributes partition ranges to workers
- Monitors worker status

### Worker

- Reads input data from multiple directories
- Performs local sorting
- Partitions data by key ranges
- Shuffles data with other workers
- Merges received partitions and writes output

---

## Technology Stack

- **Language**: Scala 2.13
- **Build Tool**: SBT
- **Networking**: gRPC + Protocol Buffers
- **Data Generation**: gensort

---

## Usage

### Start Master

```bash
master <number_of_workers>
# Output: Master IP:port and worker IP ordering
```

### Start Worker

```bash
worker <master_ip:port> -I <input_dir1> <input_dir2> ... -O <output_dir>
# Output files: partition.n, partition.n+1, partition.n+2, ...
```

---

## Project Timeline

| Week | Dates        | Milestone              |
| ---- | ------------ | ---------------------- |
| 1-2  | Oct 13-26    | Basic Infrastructure   |
| 3-4  | Oct 27-Nov 9 | Core Sorting Logic     |
| 5    | Nov 10-16    | Distributed Operations |
| 6-7  | Nov 17-30    | Fault Tolerance        |
| 8    | Dec 1-7      | Testing & Optimization |

### Key Deadlines

- **Nov 16**: Progress Report Due
- **Week 6**: Progress Presentation
- **Dec 7**: Final Submission
- **Week 9**: Final Presentation

---

## Weekly Progress

See detailed progress in [docs/](docs/) folder:

- [Week 1](docs/Week1.md) - Oct 13-19
- [Week 2](docs/Week2.md) - Oct 20-26
- ...

---

## Development Principles

- **Correctness** over performance
- **Simplicity** over premature optimization
- **Document** all design decisions
- Test incrementally with small datasets first

---

## Resources

- **gensort**: http://www.ordinal.com/gensort.html
- **Project Spec**: [Course Website](http://pl.postech.ac.kr/~gla/cs332/schedule.html)
- **gRPC Scala**: https://scalapb.github.io/

---

## Repository Structure

```
332project/
├── src/
│   ├── main/
│   │   ├── scala/          # Source code
│   │   └── protobuf/       # gRPC protocol definitions
│   └── test/               # Test code
├── docs/
│   ├── Week1.md
│   ├── Week2.md
│   └── ...
├── data/                   # Test data (gitignore)
│   ├── input1/
│   ├── input2/
│   └── input3/
├── build.sbt
└── README.md
```
