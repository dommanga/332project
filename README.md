# 332 Distributed Sorting Project

## Team Information
- Team Members: leejm21, tedoh7, sys030610

---

## Week 1 Progress (Week of 1013)

### What We Did This Week
- Formed project team
- Reviewed project requirements and specifications
- Set up Git repository
- Studied distributed sorting algorithms
- Reviewed gensort tool documentation
- Initial discussion on system architecture

### Challenges/Issues
- Need to finalize technology stack decision (gRPC vs Netty vs java.net)
- Need to set up development environment

---

## Week 2 Goals (Milestone #1 Target)

### Team Goals
1. **Environment Setup**
   - Install and test gensort tool
   - Generate sample input data (small scale: ~100MB)
   - Set up Scala development environment
   - Decide on network library (gRPC recommended)

2. **Basic Implementation**
   - Implement basic Master class skeleton
   - Implement basic Worker class skeleton
   - Test Master-Worker connection
   - Master can print IP:port
   - Worker can connect to Master

3. **Learning**
   - Learn chosen network library (gRPC + Protobuf)
   - Study key comparison logic for binary data
   - Understand 100-byte record structure (10-byte key + 90-byte value)

### Individual Responsibilities

#### leejm21
- Set up gensort and generate test data
- Research gRPC/Protobuf basics
- Implement Master skeleton code

#### tedoh7
- Set up development environment
- Implement Worker skeleton code
- Study file I/O for binary data

#### sys030610
- Design system architecture document
- Research key comparison algorithm
- Set up network communication protocol

---

## Overall Project Plan

### Phase 1: Basic Infrastructure (Week 1-2)
- Environment setup
- Master-Worker connection
- Basic network communication

### Phase 2: Core Sorting Logic (Week 3-4)
- Sampling implementation
- Local sort and partition
- Key range distribution

### Phase 3: Distributed Operations (Week 5)
- Shuffle implementation
- Data transfer between workers
- Merge implementation
- **Progress Report Due: Nov 16**

### Phase 4: Fault Tolerance (Week 6-7)
- Worker failure handling
- State recovery mechanism
- Testing with worker crashes

### Phase 5: Testing & Optimization (Week 8)
- Integration testing
- Performance tuning
- Bug fixes
- **Final Submission: Dec 7**

---

## Technical Decisions to Make

### Priority Decisions (Week 2)
- [ ] Network library choice: gRPC (recommended) vs Netty vs java.net
- [ ] Binary data handling approach
- [ ] Temporary file management strategy

### Future Decisions
- [ ] Partition count strategy
- [ ] Sampling size/method
- [ ] Thread pool sizing
- [ ] Fault tolerance mechanism

---

## Architecture Notes (Initial)

### High-Level Flow
```
1. Workers → Master: Send sample data
2. Master: Calculate partition boundaries
3. Master → Workers: Broadcast partition info
4. Workers: Sort local data, partition by ranges
5. Workers ↔ Workers: Shuffle partitions
6. Workers: Merge and write final output
```

### Key Components
- **Master**: Coordination, partition boundary calculation
- **Worker**: Sort, partition, shuffle, merge
- **Communication**: gRPC services for Master-Worker and Worker-Worker

---

## Resources
- gensort: http://www.ordinal.com/gensort.html
- Project specification: [Course website](http://pl.postech.ac.kr/~gla/cs332/schedule.html)
- gRPC Scala: https://scalapb.github.io/

---

## Notes
- Remember: Simplicity first, optimization later
- Focus on correctness over performance initially
- Document all design decisions
- Ask TA for clarification when needed
