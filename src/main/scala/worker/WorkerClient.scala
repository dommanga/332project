package worker

import rpc.sort._
import scala.concurrent.{Future, Await, ExecutionContext, TimeoutException}
import scala.concurrent.duration._
import io.grpc.ManagedChannelBuilder
import io.grpc.stub.StreamObserver
import com.google.protobuf.ByteString
import common.RecordIO

/** Worker 실행 초기 설정 */
final case class WorkerConfig(
    masterAddr: String,
    inputPaths: Seq[String],
    outputDir: String,
)

/** Worker 실행 메인 */
object WorkerClient {

  private val StateDir: String = ".worker_state"

  // ===== Fault Injector =====
  object FaultInjector {
    private val enabledPhases = sys.env.get("FAULT_INJECT_PHASE")
      .map(_.split(",").toSet)
      .getOrElse(Set.empty)
    
    private val targetWorkerId = sys.env.get("FAULT_INJECT_WORKER")
      .map(_.toInt)
      .getOrElse(-1)
    
    def checkAndCrash(phase: String): Unit = {
      if (enabledPhases.contains(phase)) {
        val myId = WorkerState.getWorkerId
        
        if (targetWorkerId == -1 || targetWorkerId == myId) {
          Console.err.println(s"\n💥💥💥 [FAULT INJECTION] 💥💥💥💥💥💥💥💥💥")
          Console.err.println(s"💥 Worker $myId crashing at phase: $phase 💥")
          Console.err.println(s"💥 Terminating in 2 seconds...                 💥")
          Console.err.println(s"💥💥💥💥💥💥💥💥💥💥💥💥💥💥💥💥💥💥💥💥\n")
          Thread.sleep(2000)
          System.exit(137)
        }
      }
    }
    
    def maybeSleep(phase: String, seconds: Int = 10): Unit = {
      if (enabledPhases.contains(phase)) {
        val myId = WorkerState.getWorkerId
        
        if (targetWorkerId == -1 || targetWorkerId == myId) {
          println(s"⏸️  [FAULT INJECTION] Worker $myId sleeping ${seconds}s at phase: $phase")
          Thread.sleep(seconds * 1000)
        }
      }
    }
  }

  // ===== Heartbeat Manager =====
  object HeartbeatManager {
    private var thread: Thread = _
    
    def start(workerInfo: WorkerInfo, masterClient: MasterClient): Unit = {
      thread = new Thread {
        setDaemon(true)
        override def run(): Unit = {
          while (!Thread.currentThread().isInterrupted) {
            try {
              masterClient.sendHeartbeat(workerInfo)
              Thread.sleep(2000)
            } catch {
              case _: InterruptedException => return
              case e: Exception => 
                Console.err.println(s"⚠️ Heartbeat error: ${e.getMessage}")
                Thread.sleep(2000)
            }
          }
        }
      }
      thread.start()
      println("💓 Heartbeat started")
    }
  
    
    def stop(): Unit = {
      if (thread != null && thread.isAlive) {
        thread.interrupt()
        thread.join(1000)
        println("💓 Heartbeat stopped")
      }
    }
  }

  /**
  * 병렬 정렬: 데이터를 numThreads개로 나눠서 병렬 정렬 후 K-way merge
  */
  private def parallelSort(
    records: Vector[Array[Byte]], 
    numThreads: Int = 4
  )(implicit ec: ExecutionContext): Vector[Array[Byte]] = {
    
    if (records.isEmpty) return Vector.empty
    
    println(s"🔧 Parallel sorting ${records.size} records with $numThreads threads...")
    
    val chunkSize = (records.size + numThreads - 1) / numThreads
    val chunks = records.grouped(chunkSize).toVector
    
    val sortedChunksFutures = chunks.map { chunk =>
      Future {
        chunk.sortWith { (a, b) =>
          RecordIO.compareKeys(extractKey(a), extractKey(b)) < 0
        }
      }
    }
    
    val sortedChunks = Await.result(Future.sequence(sortedChunksFutures), Duration.Inf)
    kWayMerge(sortedChunks.toList)
  }

  /**
  * K-way merge for sorted chunks
  */
  private def kWayMerge(chunks: List[Vector[Array[Byte]]]): Vector[Array[Byte]] = {
    case class ChunkIter(var current: Array[Byte], it: Iterator[Array[Byte]], chunkId: Int)
    
    implicit val chunkOrdering: Ordering[ChunkIter] =
      Ordering.fromLessThan[ChunkIter] { (x, y) =>
        RecordIO.compareKeys(extractKey(x.current), extractKey(y.current)) > 0
      }
    
    val pq = scala.collection.mutable.PriorityQueue.empty[ChunkIter]
    
    chunks.zipWithIndex.foreach { case (chunk, idx) =>
      val it = chunk.iterator
      if (it.hasNext) {
        pq.enqueue(ChunkIter(it.next(), it, idx))
      }
    }
    
    val result = scala.collection.mutable.ArrayBuffer.empty[Array[Byte]]
    
    while (pq.nonEmpty) {
      val smallest = pq.dequeue()
      result += smallest.current
      
      if (smallest.it.hasNext) {
        smallest.current = smallest.it.next()
        pq.enqueue(smallest)
      }
    }
    
    result.toVector
  }

  /**
  * Extract key from 100-byte record
  */
  private def extractKey(rec: Array[Byte]): Array[Byte] =
    java.util.Arrays.copyOfRange(rec, 0, RecordIO.KeySize)

  private def readAll(path: String): Vector[Array[Byte]] = {
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

  private def hasSentCheckpoints(outputDir: String): Boolean = {
    val checkpointDir = new java.io.File(s"$outputDir/sent-checkpoint")
    checkpointDir.exists() && 
      checkpointDir.listFiles().exists(_.getName.startsWith("sent_p"))
  }

  private def getSentPartitionsFromCheckpoint(outputDir: String): Seq[PartitionSendRecord] = {
    val checkpointDir = new java.io.File(s"$outputDir/sent-checkpoint")
    
    if (!checkpointDir.exists()) {
      return Seq.empty
    }
    
    checkpointDir.listFiles()
      .filter(_.getName.startsWith("sent_p"))
      .filter(_.getName.endsWith(".dat"))
      .map { file =>
        // sent_p5.dat -> 5
        val partitionId = file.getName
          .stripPrefix("sent_p")
          .stripSuffix(".dat")
          .toInt
        
        val targetWorker = WorkerState.getPartitionTargetWorker(partitionId)
        
        PartitionSendRecord(
          partitionId = partitionId,
          targetWorkerId = targetWorker,
          senderId = WorkerState.getWorkerId,
          success = true
        )
      }
      .toSeq
  }

  // ===== Main Entry Point =====
  def main(args: Array[String]): Unit = {

    implicit val ec: ExecutionContext = ExecutionContext.global

    sys.addShutdownHook {
      println("🛑 Shutting down worker...")
      HeartbeatManager.stop()
    }
    
    try {
      val conf = parseArgs(args) match {
        case Some(c) => c
        case None =>
          System.exit(1)
          return
      }

      val previousPort = loadSavedPort().getOrElse(0)

      val workerServer = new WorkerServer(previousPort, conf.outputDir)
      val actualPort   = workerServer.start()

      savePort(actualPort)

      val masterAddr = conf.masterAddr.split(":")
      val workerInfo = WorkerInfo(
        id = -1,
        ip = getLocalIP(),
        port = actualPort,
        inputDirs = conf.inputPaths,
        outputDir = conf.outputDir
      )
      
      val masterClient = new MasterClient(masterAddr(0), masterAddr(1).toInt)

      val assignment = masterClient.register(workerInfo)
      val updatedWorkerInfo = workerInfo.copy(
        id = assignment.workerId,
        port = actualPort
      )

      println(s"🚀 Worker ${assignment.workerId} registered")
      
      WorkerState.setWorkerInfo(updatedWorkerInfo)
      WorkerState.setMasterClient(masterClient)
      
      HeartbeatManager.start(updatedWorkerInfo, masterClient)

      if (hasSentCheckpoints(conf.outputDir)) {
        Thread.sleep(2000)
        println("🔄 Recovery mode: waiting for finalize...")

        try {
          val sendRecords = getSentPartitionsFromCheckpoint(conf.outputDir)
          val report = ShuffleCompletionReport(
            workerId = WorkerState.getWorkerId,
            sendRecords = sendRecords
          )
          masterClient.reportShuffleComplete(report)
          println(s"✅ Reported shuffle completion from checkpoint (${sendRecords.size} partitions)")
        } catch {
          case e: Exception =>
            Console.err.println(s"⚠️ Failed to report shuffle: ${e.getMessage}")
        }
        
        WorkerState.awaitFinalizeComplete()
        
        println("✅ Worker work completed")
        println("⏳ Waiting for shutdown...")
        WorkerState.awaitShutdownCommand()
        
        HeartbeatManager.stop()
        masterClient.shutdown()
        
        // Cleanup temporary directories
        cleanupTempDirectories(conf.outputDir)
        
        println("💀 Worker shutting down...")
        return
      }

      // Sampling
      val samples = common.Sampling.uniformEveryN(conf.inputPaths, everyN = 1000)
      println(s"📊 Collected ${samples.size} samples")

      FaultInjector.checkAndCrash("after-sampling")

      masterClient.sendSamples(samples)
      println(s"✅ Sampling complete")

      // Load and Sort
      val allRecords: Vector[Array[Byte]] =
        conf.inputPaths.flatMap(path => readAll(path)).toVector

      println(s"📦 Loaded ${allRecords.size} records")

      val sorted = parallelSort(allRecords, numThreads = 4)
      println("✅ Local sorting completed")

      FaultInjector.checkAndCrash("after-sort")

      // Partitioning
      val splitterKeys: Array[Array[Byte]] = WorkerState.getSplitters
      val partitioned = sorted.groupBy(rec => WorkerState.findPartitionId(extractKey(rec)))

      println(s"🧩 Partitioned into ${partitioned.size} partitions")

      FaultInjector.checkAndCrash("after-partition")

      // Shuffle
      val workerAddresses = WorkerState.getWorkerAddresses.getOrElse {
        throw new RuntimeException("Worker addresses not available")
      }
      
      def sendPartitionWithRetry(
        originalTarget: Int,
        partitionId: Int,
        records: Seq[Array[Byte]],
        workerAddresses: Map[Int, (String, Int)],
        maxRetries: Int = 3
      ): Boolean = {
        
        var attempt = 0
        
        while (attempt < maxRetries) {              
          try {
            val (targetIp, targetPort) = workerAddresses(originalTarget)
            
            val channel = ManagedChannelBuilder
              .forAddress(targetIp, targetPort)
              .usePlaintext()
              .build()

            try {
              val stub = WorkerServiceGrpc.stub(channel)
              val ackPromise = scala.concurrent.Promise[Unit]()
              
              val responseObserver = new StreamObserver[Ack] {
                override def onNext(v: Ack): Unit = ()
                
                override def onError(t: Throwable): Unit = {
                  ackPromise.failure(t)
                }
                
                override def onCompleted(): Unit = {
                  ackPromise.success(())
                }
              }
              
              val requestObserver = stub.pushPartition(responseObserver)
              
              var seq: Long = 0
              records.foreach { rec =>
                val chunk = PartitionChunk(
                  task = Some(TaskId("task-001")),
                  partitionId = s"p$partitionId",
                  senderId = WorkerState.getWorkerId,
                  payload = ByteString.copyFrom(rec),
                  seq = seq
                )
                seq += 1
                requestObserver.onNext(chunk)
              }
              
              requestObserver.onCompleted()
              Await.result(ackPromise.future, 10.seconds)
              channel.shutdown()
              
              return true

            } finally {
              channel.shutdown()
            }
            
          } catch {
            case e: Exception =>
              attempt += 1
              
              if (attempt < maxRetries) {
                val backoff = 2000 * attempt
                Thread.sleep(backoff)
              } else {
                Console.err.println(s"⚠️ Failed to send p$partitionId after $maxRetries attempts")
                return false
              }
          }
        }

        false
      }

      println("🚚 Starting shuffle...")

      try {      
        val maxParallel = 4
        val batches = partitioned.toSeq.grouped(maxParallel).toSeq
        
        println(s"📦 Sending ${partitioned.size} partitions in ${batches.size} batches")
        
        batches.zipWithIndex.foreach { case (batch, batchIdx) =>
          if (batchIdx == batches.size / 2) {
            FaultInjector.checkAndCrash("mid-shuffle")
          }
          
          val batchFutures = batch.map { case (pid, recs) =>
            Future {
              val targetWorker = WorkerState.getPartitionTargetWorker(pid)
              checkpointSentPartition(pid, recs, conf.outputDir)
              val success = sendPartitionWithRetry(targetWorker, pid, recs, workerAddresses)
              (pid, success)
            }
          }
          
          val results = Await.result(Future.sequence(batchFutures), 45.seconds)
          val (successes, failures) = results.partition(_._2)

          if (failures.nonEmpty) {
            println(s"⚠️  Batch ${batchIdx + 1}: ${failures.size} failed (will recover)")
          }
        }

      } catch {
        case e: Exception =>
          Console.err.println(s"⚠️ Shuffle errors: ${e.getMessage}")
          e.printStackTrace()
      }

      println("✅ Shuffle completed")

      val sendRecords = partitioned.keys.map { pid =>
        val target = WorkerState.getPartitionTargetWorker(pid)
        PartitionSendRecord(
          partitionId = pid,
          targetWorkerId = target,
          senderId = WorkerState.getWorkerId,
          success = true
        )
      }.toSeq

      val report = ShuffleCompletionReport(
        workerId = WorkerState.getWorkerId,
        sendRecords = sendRecords
      )
      WorkerState.setShuffleReport(report)

      FaultInjector.checkAndCrash("before-finalize")

      try {
        WorkerState.reportShuffleComplete()
      } catch {
        case e: Exception =>
          Console.err.println(s"⚠️ Failed to report shuffle: ${e.getMessage}")
      }

      WorkerState.awaitFinalizeComplete()

      println("✅ Worker work completed")
      println("⏳ Waiting for shutdown...")

      WorkerState.awaitShutdownCommand()

      HeartbeatManager.stop()

      try {
        masterClient.shutdown()
      } catch {
        case e: Exception =>
          Console.err.println(s"⚠️ Shutdown error: ${e.getMessage}")
      }

      // Cleanup temporary directories
      cleanupTempDirectories(conf.outputDir)

      println("💀 Worker shutting down...")
        
    } catch {
      case e: Exception =>
        Console.err.println(s"❌ Worker error: ${e.getMessage}")
        e.printStackTrace()
        HeartbeatManager.stop()

        try {
          WorkerState.getMasterClient.shutdown()
        } catch {
          case _: Exception => // Ignore
        }

        System.exit(1)
    }
  }

  /** Local IPv4 검색 */
  private def getLocalIP(): String = {
    import java.net.{InetAddress, NetworkInterface}
    import scala.jdk.CollectionConverters._

    NetworkInterface.getNetworkInterfaces.asScala
      .flatMap(_.getInetAddresses.asScala)
      .find(addr => !addr.isLoopbackAddress && addr.getAddress.length == 4)
      .map(_.getHostAddress)
      .getOrElse("127.0.0.1")
  }

  /**
   * Sender checkpoint 저장
   */
  private def checkpointSentPartition(
    partitionId: Int, 
    records: Seq[Array[Byte]], 
    outputDir: String
  ): Unit = {
    val checkpointDir = new java.io.File(s"$outputDir/sent-checkpoint")
    checkpointDir.mkdirs()
    
    val tempFile = new java.io.File(checkpointDir, s"sent_p${partitionId}.dat.tmp")
    val fos = new java.io.FileOutputStream(tempFile)
    try {
      records.foreach { rec => fos.write(rec) }
    } finally {
      fos.close()
    }
    
    val finalFile = new java.io.File(checkpointDir, s"sent_p${partitionId}.dat")
    if (finalFile.exists()) finalFile.delete()
    tempFile.renameTo(finalFile)
  }

  /**
   * Cleanup temporary checkpoint directories
   */
  private def cleanupTempDirectories(outputDir: String): Unit = {
    try {
      val sentCheckpointDir = new java.io.File(s"$outputDir/sent-checkpoint")
      val shuffleCheckpointDir = new java.io.File(s"$outputDir/shuffle-checkpoint")
      
      def deleteRecursively(file: java.io.File): Unit = {
        if (file.isDirectory) {
          file.listFiles().foreach(deleteRecursively)
        }
        file.delete()
      }
      
      if (sentCheckpointDir.exists()) {
        deleteRecursively(sentCheckpointDir)
      }
      
      if (shuffleCheckpointDir.exists()) {
        deleteRecursively(shuffleCheckpointDir)
      }
    } catch {
      case e: Exception =>
        Console.err.println(s"⚠️ Failed to cleanup temp directories: ${e.getMessage}")
    }
  }

  // ---------------------------------------------------------
  // CLI 입력 파서
  // ---------------------------------------------------------
  private def parseArgs(args: Array[String]): Option[WorkerConfig] = {
    if (args.isEmpty) {
      printUsage()
      return None
    }

    val masterAddr = args(0)
    val inputs     = collection.mutable.ArrayBuffer.empty[String]
    var outputDir  = "./out"

    var i = 1
    def needValue(opt: String): Boolean = {
      if (i >= args.length) {
        Console.err.println(s"Missing value for $opt")
        false
      } else true
    }

    while (i < args.length) {
      args(i) match {
        case "-I" | "--input" =>
          i += 1
          if (!needValue("-I")) return None
          inputs += args(i)

        case "-O" | "--output" =>
          i += 1
          if (!needValue("-O")) return None
          outputDir = args(i)

        case other =>
          Console.err.println(s"Unknown option: $other")
          printUsage()
          return None
      }
      i += 1
    }

    if (inputs.isEmpty) {
      Console.err.println("At least one -I <input-path> is required.")
      printUsage()
      None
    } else {
      Some(
        WorkerConfig(
          masterAddr = masterAddr,
          inputPaths = inputs.toSeq,
          outputDir  = outputDir
        )
      )
    }
  }

  private def printUsage(): Unit = {
    val msg =
      """Usage:
        |  worker <master IP:port> -I <input directory> [<input directory> ...] -O <output directory>
        |
        |Example:
        |  worker 141.223.91.80:30040 -I /data1/input /data2/input -O /home/gla/data
        |""".stripMargin
    Console.err.println(msg)
  }

  // ---------------------------------------------------------
  // Port persistence helpers (stateful restart)
  // ---------------------------------------------------------
  private def loadSavedPort(): Option[Int] = {
    val dir = new java.io.File(StateDir)
    val f   = new java.io.File(dir, "worker-port.state")

    if (!f.exists()) return None

    try {
      val src  = scala.io.Source.fromFile(f)
      try {
        val text = src.getLines().mkString.trim
        if (text.nonEmpty) Some(text.toInt) else None
      } finally {
        src.close()
      }
    } catch {
      case _: Throwable => None
    }
  }

  private def savePort(port: Int): Unit = {
    val dir = new java.io.File(StateDir)
    dir.mkdirs()

    val f  = new java.io.File(dir, "worker-port.state")
    val pw = new java.io.PrintWriter(f)
    try {
      pw.println(port.toString)
    } finally {
      pw.close()
    }
  }

}