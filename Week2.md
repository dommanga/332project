# Week 2 Progress (Oct 20 - Oct 26, 2025)

## This Week's Progress

### What We Completed

#### 1. Decision

- Decided on network library: gRPC + Protocol Buffers
  - Reasoning: Officially recommended, good docs, type-safe
  - Alternatives considered: Netty (too complex), java.net (too low-level)

#### 2. Learning Phase

- **Learned gRPC + Protobuf basics**
  - Service definitions and RPC patterns
  - Message definitions for Master-Worker communication
  - Async/Future patterns in gRPC Scala
- **Studied key comparison logic for binary data**
  - Byte-by-byte comparison (unsigned)
  - KeyA < KeyB if KeyA[i] == KeyB[i] for all i < k, and KeyA[k] < KeyB[k]
- **Understood 100-byte record structure**
  - First 10 bytes: key (for comparison)
  - Remaining 90 bytes: value (not used in sorting)
  - Binary data handling in Scala

#### 3. Design & Planning

- Set up initial project structure

```
  project/
  ├── src/main/scala/
  ├── src/main/protobuf/
  ├── src/test/
  ├── build.sbt
  └── docs/
```

- Discussed Master-Worker communication protocol
  - Worker registration flow
  - Sample data transmission format
  - Partition boundary distribution method

## Challenges/Issues

### Technical Challenges

- Binary data handling complexity
  - Need careful byte ordering
  - Unsigned byte comparison in Scala (bytes are signed by default)

### Team Coordination

- Time management with other coursework

## Next Week's Goals (Week 3)

### Team Goals - Milestone #1: Basic Master-Worker Connection

#### 1. Implement Basic Master (Jimin)

- Accept worker connections via gRPC
- Print Master IP:port on startup
- Maintain list of connected workers
- Handle worker registration

#### 2. Implement Basic Worker (Sangwon)

- Connect to Master using IP:port argument
- Parse command-line arguments (-I, -O flags)
- Read input files from specified directories
- Basic file I/O testing with 100-byte records

#### 3. Define Communication Protocol (Youngseo)

- Write .proto files for gRPC services
- Define messages: WorkerRegistration, SampleData, PartitionInfo
- Generate Scala code from protobuf definitions
- Document protocol design

#### 4. Integration Test

- Master + 2-3 Workers successfully connect
- Workers can send dummy messages to Master
- Verify bidirectional communication works

### Individual Responsibilities

#### Jimin

- Implement Master gRPC server
- Handle worker registration requests
- Test with multiple simultaneous worker connections
- Document Master API

#### Sangwon

- Implement Worker gRPC client
- Implement file reading from input directories
- Test reading 100-byte records correctly
- Validate key extraction (first 10 bytes)

#### Youngseo

- Complete protobuf service definitions
- Set up gRPC service interfaces
- Create integration test scenarios
- Write communication protocol documentation

## Key Decisions Made

- **Network library**: gRPC + Protocol Buffers (final decision)
- **Development environment**: Scala 2.13 + SBT + IntelliJ
- **Testing approach**: Start with small datasets (10-100MB)
- **Architecture style**: Master as gRPC server, Workers as gRPC clients

## Technical Notes

### Protobuf Service Design (Draft)

```protobuf
service MasterService {
  rpc RegisterWorker(WorkerInfo) returns (RegistrationResponse);
  rpc SendSamples(SampleData) returns (Ack);
}

service WorkerService {
  rpc ReceivePartitionInfo(PartitionRanges) returns (Ack);
  rpc ReceivePartitionData(PartitionChunk) returns (Ack);
}
```

### Key Comparison Implementation Plan

```scala
// Compare two 10-byte keys
def compareKeys(key1: Array[Byte], key2: Array[Byte]): Int = {
  for (i <- 0 until 10) {
    val b1 = key1(i) & 0xFF  // Convert to unsigned
    val b2 = key2(i) & 0xFF
    if (b1 != b2) return b1 - b2
  }
  0  // Equal
}
```

### Record Reading Strategy

```scala
// Read 100-byte records from binary file
val recordSize = 100
val buffer = new Array[Byte](recordSize)
while (inputStream.read(buffer) == recordSize) {
  val key = buffer.slice(0, 10)
  val value = buffer.slice(10, 100)
  // Process record...
}
```
