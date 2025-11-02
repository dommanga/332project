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

| Week | Dates        | Phase                  |
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
├── build.sbt
└── README.md
```
