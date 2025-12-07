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

  private val recvDir = {
    val dir = new java.io.File(s"$outputDir/recv")
    dir.mkdirs()
    dir
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
      private var countChunks: Long       = 0L
      private var currentPid: Option[String] = None
      private var senderId: Int           = -1
      private var fos: FileOutputStream   = _

      private def ensureOutputStream(partitionId: String, senderId: Int): Unit = {
        if (fos == null) {
          val runId   = s"${partitionId}_from_w$senderId"      // 예: p3_from_w2
          val tmpFile = new java.io.File(recvDir, s"$runId.dat.tmp")
          fos = new FileOutputStream(tmpFile)
        }
      }

      override def onNext(ch: PartitionChunk): Unit = {
        countChunks += 1

        if (currentPid.isEmpty) {
          currentPid = Some(ch.partitionId)
          senderId = ch.senderId
          ensureOutputStream(ch.partitionId, senderId)
        }

        val bytes = ch.payload.toByteArray
        fos.write(bytes)
      }

      override def onError(t: Throwable): Unit = {
        Console.err.println(s"❌ pushPartition error: ${t.getMessage}")
        try {
          if (fos != null) fos.close()
        } catch {
          case _: Exception =>
        }
        responseObserver.onError(t)
      }

      override def onCompleted(): Unit = {
        val pid = currentPid.getOrElse("")
        try {
          if (fos != null) fos.close()
        } catch {
          case _: Exception =>
        }

        if (pid.nonEmpty && fos != null) {
          val runId   = s"${pid}_from_w$senderId"
          val tmpFile = new java.io.File(recvDir, s"$runId.dat.tmp")
          val finalFile = new java.io.File(recvDir, s"$runId.dat")
          if (finalFile.exists()) finalFile.delete()
          tmpFile.renameTo(finalFile)
          println(s"📦 Received $pid from w$senderId -> ${finalFile.getName} (${finalFile.length()} bytes)")
        }

        responseObserver.onNext(Ack(ok = true, msg = s"Received $countChunks chunks"))
        responseObserver.onCompleted()
      }
    }
  }

  private def keyOf(rec: Array[Byte]): Array[Byte] =
    java.util.Arrays.copyOfRange(rec, 0, RecordIO.KeySize)

  private def compareRecords(a: Array[Byte], b: Array[Byte]): Int =
    RecordIO.compareKeys(keyOf(a), keyOf(b))

  private def recordIteratorFromFile(file: java.io.File): Iterator[Array[Byte]] = {
    new Iterator[Array[Byte]] {
      private val is  = new java.io.FileInputStream(file)
      private val ch  = is.getChannel
      private val recSize = RecordIO.RecordSize
      private val buf = java.nio.ByteBuffer.allocate(recSize)
      private var nextRec: Array[Byte] = readNextRecord()

      private def readNextRecord(): Array[Byte] = {
        buf.clear()
        val n = ch.read(buf)
        if (n == recSize) {
          buf.flip()
          val arr = new Array[Byte](recSize)
          buf.get(arr)
          arr
        } else {
          ch.close()
          is.close()
          null
        }
      }

      override def hasNext: Boolean = nextRec != null

      override def next(): Array[Byte] = {
        if (nextRec == null) throw new NoSuchElementException("No more records")
        val result = nextRec
        nextRec = readNextRecord()
        result
      }
    }
  }

  private def mergeRunsFromFiles(files: List[java.io.File]): Iterator[Array[Byte]] = {
    case class RunIter(var current: Array[Byte], it: Iterator[Array[Byte]])

    implicit val runOrdering: Ordering[RunIter] =
      Ordering.fromLessThan[RunIter] { (x, y) =>
        compareRecords(x.current, y.current) > 0
      }

    val pq = mutable.PriorityQueue.empty[RunIter]

    files.foreach { file =>
      val it = recordIteratorFromFile(file)
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
    val files = getRunFilesForPartition(partitionId)

    if (files.isEmpty) {
      System.err.println(s"❌ FATAL: No runs found for $partitionId even after recovery")
      throw new RuntimeException(s"Missing partition data for $partitionId")
    }

    val mergedIter: Iterator[Array[Byte]] = mergeRunsFromFiles(files)
    writePartitionToFile(partitionId, mergedIter)
  }

  def finalizeAll(): Unit = {
    val myPartitions = WorkerState.getMyPartitions.map(pid => s"p$pid")
    
    println(s"🔧 Finalizing ${myPartitions.size} partitions...")
    
    myPartitions.foreach { pidStr =>
      finalizePartition(pidStr)
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
      val receivedSenders = getReceivedSendersForPartition(pid)  // ✅ 디스크 기반

      val missingSenders = expectedSenders -- receivedSenders
      
      if (missingSenders.nonEmpty) {
        println(s"⚠️  p$pid missing from workers: $missingSenders")
        
        missingSenders.foreach { senderId =>
          requestPartitionFromWorker(senderId, pid)
        }
      }
    }
  }

  // ✅ 특정 partition 에 대해, 어떤 sender 들의 파일이 있는지 확인
  private def getReceivedSendersForPartition(partitionId: Int): Set[Int] = {
    val prefix = s"p${partitionId}_from_w"
    val files = Option(recvDir.listFiles()).getOrElse(Array.empty[java.io.File])

    files
      .filter(f => f.getName.startsWith(prefix) && f.getName.endsWith(".dat"))
      .map { f =>
        // p3_from_w2.dat → "2"
        f.getName
          .stripPrefix(prefix)
          .stripSuffix(".dat")
          .toInt
      }
      .toSet
  }

  // ✅ 특정 partition 에 해당하는 모든 run 파일 목록
  private def getRunFilesForPartition(partitionIdStr: String): List[java.io.File] = {
    val prefix = s"${partitionIdStr}_from_w"  // 예: "p3_from_w"
    Option(recvDir.listFiles()).getOrElse(Array.empty[java.io.File])
      .filter(f => f.getName.startsWith(prefix) && f.getName.endsWith(".dat"))
      .toList
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

              @volatile var fos: FileOutputStream = null
              val pidStr = s"p$partitionId"
              val runId  = s"${pidStr}_from_w$senderId"

              def ensureOutputStream(): Unit = {
                if (fos == null) {
                  val tmpFile = new java.io.File(recvDir, s"$runId.dat.tmp")
                  fos = new FileOutputStream(tmpFile)
                }
              }
              
              val responseObserver = new StreamObserver[PartitionChunk] {
                override def onNext(chunk: PartitionChunk): Unit = {
                  ensureOutputStream()
                  val bytes = chunk.payload.toByteArray
                  fos.write(bytes)
                }
                
                override def onError(t: Throwable): Unit = {
                  Console.err.println(s"❌ Error from w$senderId: ${t.getMessage}")
                  try {
                    if (fos != null) fos.close()
                  } catch { case _: Exception => () }
                  latch.countDown()
                }
                
                override def onCompleted(): Unit = {
                  try {
                    if (fos != null) fos.close()
                    val tmpFile   = new java.io.File(recvDir, s"$runId.dat.tmp")
                    val finalFile = new java.io.File(recvDir, s"$runId.dat")
                    if (finalFile.exists()) finalFile.delete()
                    tmpFile.renameTo(finalFile)
                    println(s"✅ Received p$partitionId from w$senderId (recovery) -> ${finalFile.getName}, ${finalFile.length()} bytes")
                  } catch {
                    case e: Exception =>
                      Console.err.println(s"⚠️ Failed to finalize recv file for $runId: ${e.getMessage}")
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