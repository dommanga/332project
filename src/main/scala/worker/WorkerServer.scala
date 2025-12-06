package worker

import io.grpc.{Server, ServerBuilder, ManagedChannelBuilder}
import rpc.sort.{
  WorkerServiceGrpc, 
  PartitionPlan, 
  Ack, 
  PartitionChunk, 
  TaskId, 
  PartitionRequest,
 }
import io.grpc.stub.StreamObserver
import scala.concurrent.{ExecutionContext, Future}

import scala.collection.mutable
import common.RecordIO
import java.nio.file.{Files, Paths, Path}
import java.io.FileOutputStream
import java.nio.ByteBuffer
import java.io.FileOutputStream
import java.nio.file.Files
import java.nio.file.Paths

object WorkerServer {
  /** 
   * Standalone entry point for testing WorkerServer independently.
   * In production, WorkerServer is instantiated by WorkerClient.
   */
  def main(args: Array[String]): Unit = {
    if (args.length < 1) {
      println("Usage: worker-server <port> [outputDir]")
      System.exit(1)
    }

    val port      = args(0).toInt
    val outputDir = if (args.length >= 2) args(1) else "./out"

    val server = new WorkerServer(port, outputDir)
    server.start()
    server.blockUntilShutdown()
  }
}

class WorkerServer(port: Int, outputDir: String) {
  private var server: Server = _
  private implicit val ec: ExecutionContext = ExecutionContext.global
  private val impl = new WorkerServiceImpl(outputDir)

  def start(): Unit = {
    server = ServerBuilder
      .forPort(port)
      .addService(WorkerServiceGrpc.bindService(impl, ec))
      .build()
      .start()
    println(s"WorkerService listening on port $port (outputDir=$outputDir)")
  }

  def blockUntilShutdown(): Unit = {
    if (server != null) server.awaitTermination()
  }
}

class WorkerServiceImpl(outputDir: String)(implicit ec: ExecutionContext)
  extends WorkerServiceGrpc.WorkerService {

  private val checkpointDir = {
    val dir = new java.io.File(s"$outputDir/shuffle-checkpoint")
    dir.mkdirs()
    dir
  }
  
  println(s"[Worker] Checkpoint dir: ${checkpointDir.getAbsolutePath}")

  // -----------------------------
  //  Partition 데이터 저장소 (In Memory)
  //   - partition_id 별로 "sorted run" 여러 개를 쌓아둠
  //   - run 하나 = Array[Array[Byte]] (각 원소가 100바이트 레코드)
  // -----------------------------
  object PartitionStore {
    // 메모리에만 저장 (disk 제거)
    private val runData = mutable.Map.empty[String, mutable.ArrayBuffer[Array[Array[Byte]]]]
    
    def addRun(partitionId: String, run: Array[Array[Byte]], checkpointDir: java.io.File): Unit = this.synchronized {
      if (runData.contains(partitionId) && runData(partitionId).nonEmpty) {
        println(s"[PartitionStore] ⚠️ Run $partitionId already exists, skipping duplicate")
        return
      }

      val runs = runData.getOrElseUpdate(partitionId, mutable.ArrayBuffer.empty)
      runs += run
      println(s"[PartitionStore] Added run to $partitionId: ${run.length} records (memory only)")
    }
    
    def loadRuns(partitionId: String): List[Array[Array[Byte]]] = this.synchronized {
      runData.get(partitionId).map(_.toList).getOrElse(Nil)
    }
    
    def drainRuns(partitionId: String): List[Array[Array[Byte]]] = this.synchronized {
      val runs = loadRuns(partitionId)
      runData.remove(partitionId)
      runs
    }
    
    def allPartitionIds: List[String] = this.synchronized {
      runData.keys.toList
    }

    def getSendersForPartition(partitionId: String): Set[Int] = this.synchronized {
      runData.keys
        .filter(_.startsWith(s"${partitionId}_from_w"))
        .map { key =>
          key.split("_from_w")(1).toInt
        }
        .toSet
    }

    def loadAllRunsForPartition(partitionId: String): List[Array[Array[Byte]]] = synchronized {
      println(s"[PartitionStore] Loading runs for $partitionId")
      println(s"[PartitionStore] Available keys: ${runData.keys.mkString(", ")}")
      
      val runs = runData.keys
        .filter(key => {
          val extracted = key.split("_from_").headOption.getOrElse("")
          extracted == partitionId
        })
        .flatMap(runId => {
          println(s"[PartitionStore]   Loading $runId: ${runData(runId).size} runs")
          runData(runId)
        })
        .toList
      
      println(s"[PartitionStore] Total runs for $partitionId: ${runs.size}")
      runs
    }
  }

  // -----------------------------
  //  PartitionPlan 설정
  // -----------------------------
  override def setPartitionPlan(plan: PartitionPlan): Future[Ack] = {
    println(s"[Worker] Received PartitionPlan for task=${plan.task.map(_.id).getOrElse("unknown")}")
    plan.ranges.zipWithIndex.foreach { case (r, idx) =>
      println(f"  range#$idx → worker=${r.targetWorker}%d, " +
        s"lo=${bytesToHex(r.lo.toByteArray)} hi=${bytesToHex(r.hi.toByteArray)}")
    }
    
    // Worker 주소 정보 저장
    if (plan.workers.nonEmpty) {
      val addresses: Map[Int, (String, Int)] = plan.workers.map { w =>
        w.workerId -> (w.ip, w.port)
      }.toMap
      
      println(s"[Worker] Received ${addresses.size} worker addresses:")
      addresses.foreach { case (id, (ip, port)) =>
        println(s"  worker#$id → $ip:$port")
      }
      
      // WorkerState에 저장하여 WorkerClient가 사용할 수 있도록 함
      WorkerState.setWorkerAddresses(addresses)
    }
    
    WorkerState.setPartitionPlan(plan)
    Future.successful(Ack(ok = true, msg = "Plan received"))
  }

  // -----------------------------
  //  Shuffle 수신: PushPartition
  //   - 한 stream = (task, partition_id)에 대한 "한 sender의 정렬된 run"
  //   - payload 안에는 여러 개의 100B record가 들어있음
  // -----------------------------
  override def pushPartition(responseObserver: StreamObserver[Ack]): StreamObserver[PartitionChunk] = {
    new StreamObserver[PartitionChunk] {
      private var countChunks: Long = 0L
      private var currentPid: Option[String] = None
      private var senderId: Int = -1
      private val recordBuffer = mutable.ArrayBuffer.empty[Array[Byte]]

      override def onNext(ch: PartitionChunk): Unit = {
        countChunks += 1
        
        if (currentPid.isEmpty) {
          currentPid = Some(ch.partitionId)
          senderId = ch.senderId
        }
        
        val bytes = ch.payload.toByteArray
        val recLen = RecordIO.RecordSize
        
        var offset = 0
        while (offset + recLen <= bytes.length) {
          val rec = java.util.Arrays.copyOfRange(bytes, offset, offset + recLen)
          recordBuffer += rec
          offset += recLen
        }
      }

      override def onError(t: Throwable): Unit = {
        Console.err.println(s"[Worker] pushPartition stream error: ${t.getMessage}")
        responseObserver.onError(t)
      }

      override def onCompleted(): Unit = {
        val pid = currentPid.getOrElse("")
        val run = recordBuffer.toArray
        
        if (pid.nonEmpty && run.nonEmpty) {
          // Include sender info
          val runId = s"${pid}_from_w${senderId}"
          PartitionStore.addRun(runId, run, checkpointDir)
          println(s"[Worker] Received $pid from worker#$senderId: ${run.length} records")
        }
        
        responseObserver.onNext(Ack(ok = true, msg = s"Received ${run.length} records"))
        responseObserver.onCompleted()
      }
    }
  }

  // -----------------------------
  //  K-way merge & 파일 쓰기 helper
  // -----------------------------

  /** record에서 key(앞 10바이트)를 뽑는 함수 */
  private def keyOf(rec: Array[Byte]): Array[Byte] =
    java.util.Arrays.copyOfRange(rec, 0, RecordIO.KeySize)

  /** record 두 개를 key 기준으로 비교 */
  private def compareRecords(a: Array[Byte], b: Array[Byte]): Int =
    RecordIO.compareKeys(keyOf(a), keyOf(b))

  /**
   * 여러 개의 "정렬된 run" (각 run은 Array[Array[Byte]])을 K-way merge 해서
   * 전체 오름차순 record iterator를 반환.
   */
  private def mergeRuns(runs: List[Array[Array[Byte]]]): Iterator[Array[Byte]] = {
    case class RunIter(var current: Array[Byte], it: Iterator[Array[Byte]])

    // Scala PriorityQueue 는 max-heap 이라, 최소 key가 먼저 나오게 비교 반전
    implicit val runOrdering: Ordering[RunIter] =
      Ordering.fromLessThan[RunIter] { (x, y) =>
        compareRecords(x.current, y.current) > 0 // current가 "더 큰"을 true로 → min-heap 효과
      }

    val pq = mutable.PriorityQueue.empty[RunIter]

    // 각 run의 첫 요소를 PQ에 넣기
    runs.foreach { runArr =>
      val it = runArr.iterator
      if (it.hasNext) {
        pq.enqueue(RunIter(it.next(), it))
      }
    }

    new Iterator[Array[Byte]] {
      override def hasNext: Boolean = pq.nonEmpty
      override def next(): Array[Byte] = {
        val smallest = pq.dequeue()
        val result   = smallest.current
        if (smallest.it.hasNext) {
          smallest.current = smallest.it.next()
          pq.enqueue(smallest)
        }
        result
      }
    }
  }

  /** partitionId에 해당하는 run들을 K-way merge 해서 파일로 쓰기 */
  def finalizePartition(partitionId: String): Unit = {
    // 모든 sender들의 runs를 load
    val runs: List[Array[Array[Byte]]] = PartitionStore.loadAllRunsForPartition(partitionId)
    
    if (runs.isEmpty) {
      println(s"[Worker] finalizePartition($partitionId): no data")
      return
    }
    
    val mergedIter: Iterator[Array[Byte]] = mergeRuns(runs)
    writePartitionToFile(partitionId, mergedIter)
  }

  /** 현재까지 들어온 모든 partition_id에 대해 finalize */
  def finalizeAll(): Unit = {
    val allRunIds = PartitionStore.allPartitionIds  // ["p2_from_w1", "p2_from_w0", ...]
    
    val uniquePartitionIds = allRunIds
      .map { runId =>
        // "p2_from_w1" → "p2"
        runId.split("_from_").headOption.getOrElse(runId)
      }
      .toSet
    
    println(s"[Worker] finalizeAll: unique partitions=${uniquePartitionIds.mkString(", ")}")
    
    uniquePartitionIds.foreach { pid =>
      finalizePartition(pid)
    }
  }

  /** 최종 merged record들을 outputDir 아래 파일로 저장 */
  private def writePartitionToFile(partitionId: String, records: Iterator[Array[Byte]]): Unit = {
    val outDirPath: Path = Paths.get(outputDir)
    if (!Files.exists(outDirPath)) {
      Files.createDirectories(outDirPath)
    }

    // partition_id가 "p0" 형태라면 "partition.0" 으로 저장
    val fileName =
      if (partitionId.startsWith("p") && partitionId.drop(1).forall(_.isDigit)) {
        s"partition.${partitionId.drop(1)}"
      } else {
        partitionId
      }

    val filePath = outDirPath.resolve(fileName)
    val fos      = new FileOutputStream(filePath.toFile)
    val ch       = fos.getChannel

    try {
      records.foreach { rec =>
        val buf = ByteBuffer.wrap(rec)
        ch.write(buf)
      }
      println(s"[Worker] Wrote ${filePath.toAbsolutePath}")
    } finally {
      ch.close()
      fos.close()
    }
  }

  // Byte array → hex 문자열 (기존 함수 유지)
  private def bytesToHex(arr: Array[Byte]): String =
    arr.map("%02X".format(_)).mkString

  override def startShuffle(taskId: TaskId): Future[Ack] = {
    Future {
      println(s"[Worker] Received StartShuffle command for task=${taskId.id}")

      Ack(ok = true, msg = "Shuffle started")
    }
  }

  override def finalizePartitions(taskId: TaskId): Future[Ack] = {
    Future {
      println(s"[Worker] Received FinalizePartitions command")
      
      // Missing Detection
      detectAndRequestMissingPartitions()
      WorkerClient.FaultInjector.checkAndCrash("finalize-1")
      finalizeAll()
      WorkerClient.FaultInjector.checkAndCrash("finalize-2")
      reportMergeCompleteToMaster()
      WorkerState.signalFinalizeComplete()
      
      Ack(ok = true, msg = "Finalize complete")
    }
  }

  private def detectAndRequestMissingPartitions(): Unit = {  
    // 내가 받아야 하는 partitions
    val expectedPartitions = WorkerState.getMyPartitions
    
    println(s"[Worker] Checking ${expectedPartitions.size} partitions for missing senders...")
    
    expectedPartitions.foreach { pid =>
      // 이 partition을 보냈어야 하는 senders (Master에게 query)
      val expectedSenders = WorkerState.getMasterClient.queryPartitionSenders(pid).toSet
      
      // 실제로 받은 senders
      val receivedSenders = PartitionStore.getSendersForPartition(s"p$pid")
      
      val missingSenders = expectedSenders -- receivedSenders
      
      if (missingSenders.nonEmpty) {
        println(s"[Worker] p$pid missing from workers: $missingSenders")
        
        // Missing sender들에게 요청
        missingSenders.foreach { senderId =>
          requestPartitionFromWorker(senderId, pid)
        }
      }
    }
    
    println("[Worker] Missing detection complete")
  }

  private def requestPartitionFromWorker(senderId: Int, partitionId: Int): Unit = {
    WorkerState.getWorkerAddresses match {
      case Some(addresses) =>
        addresses.get(senderId) match {
          case Some((ip, port)) =>
            println(s"[Worker] Requesting p$partitionId from worker#$senderId...")
            
            try {
              val channel = ManagedChannelBuilder
                .forAddress(ip, port)
                .usePlaintext()
                .build()
              
              val stub = WorkerServiceGrpc.stub(channel)
              val myId = WorkerState.getWorkerId
              val request = PartitionRequest(
                partitionId = partitionId,
                requesterId = myId
              )
              
              val latch = new java.util.concurrent.CountDownLatch(1)
              val recordBuffer = scala.collection.mutable.ArrayBuffer.empty[Array[Byte]]
              
              val responseObserver = new StreamObserver[PartitionChunk] {
                override def onNext(chunk: PartitionChunk): Unit = {
                  val bytes = chunk.payload.toByteArray
                  val recLen = RecordIO.RecordSize
                  var offset = 0
                  while (offset + recLen <= bytes.length) {
                    val rec = java.util.Arrays.copyOfRange(bytes, offset, offset + recLen)
                    recordBuffer += rec
                    offset += recLen
                  }
                }
                
                override def onError(t: Throwable): Unit = {
                  Console.err.println(s"[Worker] Error from worker#$senderId: ${t.getMessage}")
                  latch.countDown()
                }
                
                override def onCompleted(): Unit = {
                  if (recordBuffer.nonEmpty) {
                    val runId = s"p${partitionId}_from_w$senderId"
                    PartitionStore.addRun(runId, recordBuffer.toArray, checkpointDir)
                    println(s"[Worker] ✅ Received p$partitionId from worker#$senderId: ${recordBuffer.size} records")
                  }
                  latch.countDown()
                }
              }
              
              stub.requestPartition(request, responseObserver)
              latch.await(60, java.util.concurrent.TimeUnit.SECONDS)
              channel.shutdown()
              
            } catch {
              case e: Exception =>
                Console.err.println(s"[Worker] ❌ Failed to request from worker#$senderId: ${e.getMessage}")
            }
            
          case None =>
            Console.err.println(s"[Worker] Worker#$senderId address not found")
        }
        
      case None =>
        Console.err.println("[Worker] No worker addresses")
    }
  }

  private def reportMergeCompleteToMaster(): Unit = {
    try {
      WorkerState.reportMergeComplete()
      println("[Worker] ✅ Merge completion reported to Master")
    } catch {
      case e: Exception =>
        Console.err.println(s"[Worker] ⚠️ Failed to report merge completion: ${e.getMessage}")
        Console.err.println("[Worker] ⚠️ This is non-fatal - partitions already written to disk")
    }
  }

  override def requestPartition(
    request: PartitionRequest,
    responseObserver: StreamObserver[PartitionChunk]
  ): Unit = {
    val partitionId = request.partitionId
    val requesterId = request.requesterId
    
    println(s"[Worker] Partition request: p$partitionId from worker#$requesterId")
    
    try {
      val checkpointFile = new java.io.File(
        s"$outputDir/sent-checkpoint/sent_p${partitionId}.dat"
      )
      
      if (!checkpointFile.exists()) {
        println(s"[Worker]   Checkpoint missing, regenerating...")
        regenerateAndSendPartition(partitionId, responseObserver)
        return
      }
      
      println(s"[Worker]   Sending checkpoint from disk")
      val bytes = java.nio.file.Files.readAllBytes(checkpointFile.toPath)
      val recordSize = RecordIO.RecordSize
      
      val chunkSize = 1000 * recordSize
      var offset = 0
      var seq = 0L
      
      while (offset < bytes.length) {
        val end = Math.min(offset + chunkSize, bytes.length)
        val chunkBytes = java.util.Arrays.copyOfRange(bytes, offset, end)
        
        val chunk = PartitionChunk(
          task = Some(TaskId("recovery")),
          partitionId = s"p$partitionId",
          senderId = WorkerState.getWorkerId,
          payload = com.google.protobuf.ByteString.copyFrom(chunkBytes),
          seq = seq
        )
        
        responseObserver.onNext(chunk)
        offset = end
        seq += 1
      }
      
      responseObserver.onCompleted()
      println(s"[Worker]   ✅ Sent ${bytes.length / recordSize} records")
      
    } catch {
      case e: Exception =>
        Console.err.println(s"[Worker]   ❌ Error: ${e.getMessage}")
        responseObserver.onError(e)
    }
  }

  override def shutdown(taskId: TaskId): Future[Ack] = {
    Future {
      println(s"[Worker] 🛑 Received Shutdown command from Master")
      
      WorkerState.signalShutdown()
      
      Ack(ok = true, msg = "Shutdown acknowledged")
    }
  }

  private def loadCheckpointFile(file: java.io.File): Array[Array[Byte]] = {
    val bytes = java.nio.file.Files.readAllBytes(file.toPath)
    val recordSize = RecordIO.RecordSize
    val numRecords = bytes.length / recordSize
    
    (0 until numRecords).map { i =>
      java.util.Arrays.copyOfRange(bytes, i * recordSize, (i + 1) * recordSize)
    }.toArray
  }

  private def regenerateOwnPartition(partitionId: Int): Array[Array[Byte]] = {
    val inputPaths = WorkerState.getInputPaths
    if (inputPaths.isEmpty) {
      Console.err.println(s"[Worker] ⚠️ No input paths!")
      return Array.empty
    }
    
    // Input 읽기
    def readAll(path: String): Vector[Array[Byte]] = {
      val file = new java.io.File(path)
      val files = if (file.isDirectory) {
        file.listFiles().filter(_.isFile).toSeq
      } else {
        Seq(file)
      }
      
      files.flatMap { f =>
        val buf = scala.collection.mutable.ArrayBuffer.empty[Array[Byte]]
        RecordIO.streamRecords(f.getPath) { (key, value) =>
          val rec = new Array[Byte](RecordIO.RecordSize)
          System.arraycopy(key, 0, rec, 0, RecordIO.KeySize)
          System.arraycopy(value, 0, rec, RecordIO.KeySize, RecordIO.RecordSize - RecordIO.KeySize)
          buf += rec
        }
        buf
      }.toVector
    }
    
    val allRecords = inputPaths.flatMap(readAll).toVector
    println(s"[Worker]   Loaded ${allRecords.size} records")
    
    // Sort
    val sorted = allRecords.sortWith { (a, b) =>
      RecordIO.compareKeys(keyOf(a), keyOf(b)) < 0
    }
    
    // Extract partition
    val splitters = WorkerState.getSplitters
    
    val myPartitionData = sorted.filter { rec =>
      WorkerState.findPartitionId(keyOf(rec)) == partitionId
    }
    
    println(s"[Worker]   Extracted ${myPartitionData.size} records for p$partitionId")
    
    // Checkpoint 저장
    val checkpointDir = new java.io.File(s"$outputDir/sent-checkpoint")
    checkpointDir.mkdirs()
    val checkpointFile = new java.io.File(checkpointDir, s"sent_p${partitionId}.dat")
    val fos = new java.io.FileOutputStream(checkpointFile)
    try {
      myPartitionData.foreach { rec => fos.write(rec) }
    } finally {
      fos.close()
    }
    
    myPartitionData.toArray
  }

  private def requestCheckpointsFromPeers(partitionId: Int): Unit = {
    WorkerState.getWorkerAddresses match {
      case Some(addresses) =>
        val myId = WorkerState.getWorkerId
        val otherWorkers = addresses.filter { case (wid, _) => wid != myId }

        val alreadyReceived = PartitionStore.getSendersForPartition(s"p$partitionId")
        println(s"[Worker]     Already received from: $alreadyReceived")
        
        val workersToRequest = otherWorkers.filterNot { case (wid, _) => 
          alreadyReceived.contains(wid)
        }
        
        if (workersToRequest.isEmpty) {
          println(s"[Worker]     No missing data for p$partitionId")
          return
        }
        
        println(s"[Worker]     Requesting from: ${workersToRequest.keys.mkString(", ")}")
        
        
        workersToRequest.foreach { case (wid, (ip, port)) =>
          try {
            val channel = ManagedChannelBuilder
              .forAddress(ip, port)
              .usePlaintext()
              .build()
            
            val stub = WorkerServiceGrpc.stub(channel)
            val request = PartitionRequest(
              partitionId = partitionId,
              requesterId = myId
            )
            
            val latch = new java.util.concurrent.CountDownLatch(1)
            val recordBuffer = scala.collection.mutable.ArrayBuffer.empty[Array[Byte]]
            
            val responseObserver = new StreamObserver[PartitionChunk] {
              override def onNext(chunk: PartitionChunk): Unit = {
                val bytes = chunk.payload.toByteArray
                val recLen = RecordIO.RecordSize
                var offset = 0
                while (offset + recLen <= bytes.length) {
                  val rec = java.util.Arrays.copyOfRange(bytes, offset, offset + recLen)
                  recordBuffer += rec
                  offset += recLen
                }
              }
              
              override def onError(t: Throwable): Unit = {
                println(s"[Worker]     Error from worker#$wid: ${t.getMessage}")
                latch.countDown()
              }
              
              override def onCompleted(): Unit = {
                if (recordBuffer.nonEmpty) {
                  val runId = s"p${partitionId}_from_w$wid"
                  PartitionStore.addRun(runId, recordBuffer.toArray, checkpointDir)
                  println(s"[Worker]     ✅ Received ${recordBuffer.size} records from worker#$wid")
                }
                latch.countDown()
              }
            }
            
            stub.requestPartition(request, responseObserver)
            latch.await(30, java.util.concurrent.TimeUnit.SECONDS)
            channel.shutdown()
            
          } catch {
            case e: Exception =>
              println(s"[Worker]     ⚠️ Failed from worker#$wid: ${e.getMessage}")
          }
        }
        
      case None =>
        Console.err.println("[Worker] ⚠️ No worker addresses!")
    }
  }

  private def regenerateAndSendPartition(
    partitionId: Int,
    responseObserver: StreamObserver[PartitionChunk]
  ): Unit = {
    println(s"[Worker]   Regenerating p$partitionId for requester...")
    
    val data = regenerateOwnPartition(partitionId)
    val recordSize = RecordIO.RecordSize
    val chunkSize = 1000 * recordSize
    
    var offset = 0
    var seq = 0L
    
    data.grouped(1000).foreach { batch =>
      val bytes = batch.flatMap(_.toSeq).toArray
      val chunk = PartitionChunk(
        task = Some(TaskId("recovery")),
        partitionId = s"p$partitionId",
        senderId = WorkerState.getWorkerId,
        payload = com.google.protobuf.ByteString.copyFrom(bytes),
        seq = seq
      )
      responseObserver.onNext(chunk)
      seq += 1
    }
    
    responseObserver.onCompleted()
    println(s"[Worker]   ✅ Sent regenerated ${data.length} records")
  }
}