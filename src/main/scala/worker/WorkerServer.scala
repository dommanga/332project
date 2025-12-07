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

class WorkerServer(port: Int, outputDir: String) {
  private var server: Server = _
  private implicit val ec: ExecutionContext = ExecutionContext.global
  private val impl = new WorkerServiceImpl(outputDir)

  def start(): Int = {
    server = ServerBuilder
      .forPort(port)
      .addService(WorkerServiceGrpc.bindService(impl, ec))
      .build()
      .start()
    val actualPort = server.getPort

    println(s"🔌 WorkerServer started on port $actualPort")
    actualPort
  }

  def blockUntilShutdown(): Unit = {
    if (server != null) server.awaitTermination()
  }
}

class WorkerServiceImpl(outputDir: String)(implicit ec: ExecutionContext)
  extends WorkerServiceGrpc.WorkerService {
  
  @volatile private var shutdownReceived = false

  private val checkpointDir = {
    val dir = new java.io.File(s"$outputDir/shuffle-checkpoint")
    dir.mkdirs()
    dir
  }

  object PartitionStore {
    private val runData = mutable.Map.empty[String, mutable.ArrayBuffer[Array[Array[Byte]]]]
    
    def addRun(partitionId: String, run: Array[Array[Byte]], checkpointDir: java.io.File): Unit = this.synchronized {
      if (runData.contains(partitionId) && runData(partitionId).nonEmpty) {
        return
      }

      val runs = runData.getOrElseUpdate(partitionId, mutable.ArrayBuffer.empty)
      runs += run
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
      runData.keys
        .filter(key => {
          val extracted = key.split("_from_").headOption.getOrElse("")
          extracted == partitionId
        })
        .flatMap(runId => runData(runId))
        .toList
    }
  }

  override def setPartitionPlan(plan: PartitionPlan): Future[Ack] = {
    // Worker 주소 정보 저장
    WorkerState.setPartitionPlan(plan)

    if (plan.workers.nonEmpty) {
      val addresses: Map[Int, (String, Int)] = plan.workers.map { w =>
        w.workerId -> (w.ip, w.port)
      }.toMap
      
      WorkerState.setWorkerAddresses(addresses) // Important
    }
    
    Future.successful(Ack(ok = true, msg = "Plan received"))
  }

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
        Console.err.println(s"❌ pushPartition error: ${t.getMessage}")
        responseObserver.onError(t)
      }

      override def onCompleted(): Unit = {
        val pid = currentPid.getOrElse("")
        val run = recordBuffer.toArray
        
        if (pid.nonEmpty && run.nonEmpty) {
          val runId = s"${pid}_from_w${senderId}"
          PartitionStore.addRun(runId, run, checkpointDir)
          println(s"📦 Received $pid from w$senderId: ${run.length} records")
        }
        
        responseObserver.onNext(Ack(ok = true, msg = s"Received ${run.length} records"))
        responseObserver.onCompleted()
      }
    }
  }

  private def keyOf(rec: Array[Byte]): Array[Byte] =
    java.util.Arrays.copyOfRange(rec, 0, RecordIO.KeySize)

  private def compareRecords(a: Array[Byte], b: Array[Byte]): Int =
    RecordIO.compareKeys(keyOf(a), keyOf(b))

  private def mergeRuns(runs: List[Array[Array[Byte]]]): Iterator[Array[Byte]] = {
    case class RunIter(var current: Array[Byte], it: Iterator[Array[Byte]])

    implicit val runOrdering: Ordering[RunIter] =
      Ordering.fromLessThan[RunIter] { (x, y) =>
        compareRecords(x.current, y.current) > 0
      }

    val pq = mutable.PriorityQueue.empty[RunIter]

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

  def finalizePartition(partitionId: String): Unit = {
    val runs: List[Array[Array[Byte]]] = PartitionStore.loadAllRunsForPartition(partitionId)
    
    if (runs.isEmpty) {
      return
    }
    
    val mergedIter: Iterator[Array[Byte]] = mergeRuns(runs)
    writePartitionToFile(partitionId, mergedIter)
  }

  def finalizeAll(): Unit = {
    val allRunIds = PartitionStore.allPartitionIds
    
    val uniquePartitionIds = allRunIds
      .map { runId =>
        runId.split("_from_").headOption.getOrElse(runId)
      }
      .toSet
    
    println(s"🔧 Finalizing ${uniquePartitionIds.size} partitions...")
    
    uniquePartitionIds.foreach { pid =>
      finalizePartition(pid)
    }
  }

  private def writePartitionToFile(partitionId: String, records: Iterator[Array[Byte]]): Unit = {
    val outDirPath: Path = Paths.get(outputDir)
    if (!Files.exists(outDirPath)) {
      Files.createDirectories(outDirPath)
    }

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
      println(s"✅ Wrote ${filePath.getFileName}")
    } finally {
      ch.close()
      fos.close()
    }
  }

  override def startShuffle(taskId: TaskId): Future[Ack] = {
    Future.successful(Ack(ok = true, msg = "Shuffle started"))
  }

  override def finalizePartitions(taskId: TaskId): Future[Ack] = {
    Future {
      println(s"🔧 Starting finalize phase...")
      
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
    val expectedPartitions = WorkerState.getMyPartitions
    
    println(s"🔍 Checking ${expectedPartitions.size} partitions for missing data...")
    
    expectedPartitions.foreach { pid =>
      val expectedSenders = WorkerState.getMasterClient.queryPartitionSenders(pid).toSet
      val receivedSenders = PartitionStore.getSendersForPartition(s"p$pid")
      
      val missingSenders = expectedSenders -- receivedSenders
      
      if (missingSenders.nonEmpty) {
        println(s"⚠️  p$pid missing from workers: $missingSenders")
        
        missingSenders.foreach { senderId =>
          requestPartitionFromWorker(senderId, pid)
        }
      }
    }
  }

  private def requestPartitionFromWorker(senderId: Int, partitionId: Int): Unit = {
    WorkerState.getWorkerAddresses match {
      case Some(addresses) =>
        addresses.get(senderId) match {
          case Some((ip, port)) =>
            println(s"🔄 Requesting p$partitionId from w$senderId...")
            
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
                  Console.err.println(s"❌ Error from w$senderId: ${t.getMessage}")
                  latch.countDown()
                }
                
                override def onCompleted(): Unit = {
                  if (recordBuffer.nonEmpty) {
                    val runId = s"p${partitionId}_from_w$senderId"
                    PartitionStore.addRun(runId, recordBuffer.toArray, checkpointDir)
                    println(s"✅ Received p$partitionId from w$senderId: ${recordBuffer.size} records")
                  }
                  latch.countDown()
                }
              }
              
              stub.requestPartition(request, responseObserver)
              latch.await(60, java.util.concurrent.TimeUnit.SECONDS)
              channel.shutdown()
              
            } catch {
              case e: Exception =>
                Console.err.println(s"❌ Failed to request from w$senderId: ${e.getMessage}")
            }
            
          case None =>
            Console.err.println(s"❌ Worker $senderId address not found")
        }
        
      case None =>
        Console.err.println("❌ No worker addresses available")
    }
  }

  private def reportMergeCompleteToMaster(): Unit = {
    try {
      WorkerState.reportMergeComplete()
      println("✅ Merge completion reported to Master")
    } catch {
      case e: Exception =>
        Console.err.println(s"⚠️ Failed to report merge completion: ${e.getMessage}")
    }
  }

  override def requestPartition(
    request: PartitionRequest,
    responseObserver: StreamObserver[PartitionChunk]
  ): Unit = {
    val partitionId = request.partitionId
    val requesterId = request.requesterId
    
    println(s"📤 Sending p$partitionId to w$requesterId")
    
    try {
      val checkpointFile = new java.io.File(
        s"$outputDir/sent-checkpoint/sent_p${partitionId}.dat"
      )
      
      if (!checkpointFile.exists()) {
        regenerateAndSendPartition(partitionId, responseObserver)
        return
      }
      
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
      println(s"✅ Sent p$partitionId: ${bytes.length / recordSize} records")
      
    } catch {
      case e: Exception =>
        Console.err.println(s"❌ Error sending p$partitionId: ${e.getMessage}")
        responseObserver.onError(e)
    }
  }

  override def shutdown(taskId: TaskId): Future[Ack] = {
    Future {
      this.synchronized {
        if (!shutdownReceived) {
          shutdownReceived = true
          println(s"🛑 Shutdown command received")
        }
      }
      WorkerState.signalShutdown()
      Ack(ok = true, msg = "Shutdown acknowledged")
    }
  }

  private def regenerateOwnPartition(partitionId: Int): Array[Array[Byte]] = {
    val inputPaths = WorkerState.getInputPaths
    if (inputPaths.isEmpty) {
      Console.err.println(s"⚠️ No input paths available")
      return Array.empty
    }
    
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
    
    val sorted = allRecords.sortWith { (a, b) =>
      RecordIO.compareKeys(keyOf(a), keyOf(b)) < 0
    }
    
    val myPartitionData = sorted.filter { rec =>
      WorkerState.findPartitionId(keyOf(rec)) == partitionId
    }
    
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

  private def regenerateAndSendPartition(
    partitionId: Int,
    responseObserver: StreamObserver[PartitionChunk]
  ): Unit = {
    println(s"🔄 Regenerating p$partitionId...")
    
    val data = regenerateOwnPartition(partitionId)
    
    data.grouped(1000).zipWithIndex.foreach { case (batch, seq) =>
      val bytes = batch.flatMap(_.toSeq).toArray
      val chunk = PartitionChunk(
        task = Some(TaskId("recovery")),
        partitionId = s"p$partitionId",
        senderId = WorkerState.getWorkerId,
        payload = com.google.protobuf.ByteString.copyFrom(bytes),
        seq = seq.toLong
      )
      responseObserver.onNext(chunk)
    }
    
    responseObserver.onCompleted()
    println(s"✅ Sent regenerated p$partitionId: ${data.length} records")
  }
}