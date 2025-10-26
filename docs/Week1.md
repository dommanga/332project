# Week 1 Progress (Oct 13 - Oct 19, 2025)

## This Week's Progress

- Formed project team
- Reviewed project requirements and specifications thoroughly
- Set up Git repository: https://github.com/[username]/332project
- Studied distributed sorting algorithms and MapReduce-style sorting
- Reviewed gensort tool documentation
- Initial discussion on system architecture
- Analyzed project requirements:
  - Master-Worker architecture
  - 4-phase sorting: Sampling → Sort/Partition → Shuffle → Merge
  - Fault tolerance requirement
  - Input: 32MB blocks per worker
  - Output: Sorted partitions across workers

## Challenges/Issues Identified

- Need to finalize technology stack decision quickly
- Need to set up development environment

## Next Week's Goals (Week 2)

### Team Goals

1. **Environment Setup**

   - Install and test gensort tool on local machines
   - Generate sample input data (start with ~100MB for testing)
   - Set up Scala development environment (SBT, IDE configuration)
   - **Decide on network library** (gRPC strongly recommended)

2. **Learning Phase**
   - Learn chosen network library (likely gRPC + Protobuf)
   - Study key comparison logic for binary data
   - Understand 100-byte record structure (10-byte key + 90-byte value)
   - Review Scala concurrency basics (Futures, parallel collections)

### Individual Responsibilities for Week 2

#### leejm21

- Set up gensort tool and generate various test datasets
- Research gRPC/Protobuf basics and create simple examples
- Start implementing Master skeleton code
- Document gensort usage examples

#### tedoh7

- Set up Scala development environment for the team
- Start implementing Worker skeleton code
- Study file I/O for binary data in Scala
- Research efficient ways to read/write 100-byte records

#### sys030610

- Design initial system architecture document
- Research key comparison algorithm (byte-by-byte comparison)
- Set up network communication protocol design
- Create data flow diagrams for 4 phases

## Key Decisions Made

- Team composition finalized
- Git repository structure decided
- Weekly progress documentation format decided (separate files per week)

## Key Decisions Pending

- Network library choice: gRPC vs Netty vs java.net
- Project structure and package organization
