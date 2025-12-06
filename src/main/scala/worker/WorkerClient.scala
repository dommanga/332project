package worker

import rpc.sort._
import scala.concurrent.{Future, Await, ExecutionContext}
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
        
        // 특정 worker만 죽이기
        if (targetWorkerId == -1 || targetWorkerId == myId) {
          Console.err.println(s"\n💥💥💥 [FAULT INJECTION] 💥💥💥💥💥💥💥💥💥")
          Console.err.println(s"💥 Worker $myId crashing at phase: $phase 💥")
          Console.err.println(s"💥 Terminating in 2 seconds...                 💥")
          Console.err.println(s"💥💥💥💥💥💥💥💥💥💥💥💥💥💥💥💥💥💥💥💥\n")
          Thread.sleep(2000)
          System.exit(137)  // Simulate kill -9
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
              Thread.sleep(3000)
            } catch {
              case _: InterruptedException => return
              case e: Exception => 
                println(s"⚠️ Heartbeat error: ${e.getMessage}")
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
    
    println(s"🔧 Parallel sorting with $numThreads threads...")
    
    // Step 1: 데이터를 numThreads개 chunk로 분할
    val chunkSize = (records.size + numThreads - 1) / numThreads
    val chunks = records.grouped(chunkSize).toVector
    println(s"   Split into ${chunks.size} chunks (avg ${chunkSize} records/chunk)")
    
    // Step 2: 각 chunk를 병렬로 정렬
    val sortedChunksFutures = chunks.zipWithIndex.map { case (chunk, idx) =>
      Future {
        println(s"   Thread $idx: sorting ${chunk.size} records...")
        val sorted = chunk.sortWith { (a, b) =>
          RecordIO.compareKeys(extractKey(a), extractKey(b)) < 0
        }
        println(s"   Thread $idx: done")
        sorted
      }
    }
    
    val sortedChunks = Await.result(Future.sequence(sortedChunksFutures), Duration.Inf)
    println(s"   All chunks sorted, starting merge...")
    
    // Step 3: K-way merge
    val merged = kWayMerge(sortedChunks.toList)
    println(s"   Merge complete!")
    
    merged
  }

  /**
  * K-way merge for sorted chunks
  */
  private def kWayMerge(chunks: List[Vector[Array[Byte]]]): Vector[Array[Byte]] = {
    case class ChunkIter(var current: Array[Byte], it: Iterator[Array[Byte]], chunkId: Int)
    
    // Min-heap (Scala의 PriorityQueue는 max-heap이라 반전)
    implicit val chunkOrdering: Ordering[ChunkIter] =
      Ordering.fromLessThan[ChunkIter] { (x, y) =>
        RecordIO.compareKeys(extractKey(x.current), extractKey(y.current)) > 0
      }
    
    val pq = scala.collection.mutable.PriorityQueue.empty[ChunkIter]
    
    // 각 chunk의 첫 요소를 PQ에 넣기
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

  // ===== Main Entry Point =====
  def main(args: Array[String]): Unit = {

    implicit val ec: ExecutionContext = ExecutionContext.global

    // Shutdown Hook
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
      
      val masterAddr = conf.masterAddr.split(":")
      val workerInfo = WorkerInfo(
        id = -1,
        ip = getLocalIP(),
        port = 6000   // Default
      )
      
      val masterClient = new MasterClient(masterAddr(0), masterAddr(1).toInt)(
        scala.concurrent.ExecutionContext.global
      )

      val assignment = masterClient.register(workerInfo)

      println("=============================================")
      println("   ✅ Worker started with master assignment")
      println(s"      master   = ${conf.masterAddr}")
      println(s"      inputs   = ${conf.inputPaths.mkString(", ")}")
      println(s"      output   = ${conf.outputDir}")
      println(s"      id       = ${assignment.workerId}")
      println(s"      port     = ${assignment.assignedPort}")
      println("=============================================")
      
      val updatedWorkerInfo = workerInfo.copy(
        id = assignment.workerId,
        port = assignment.assignedPort
      )
      WorkerState.setWorkerInfo(updatedWorkerInfo)
      WorkerState.setMasterClient(masterClient)
      
      val workerServer = new WorkerServer(assignment.assignedPort, conf.outputDir)
      workerServer.start()
      println(s"🔌 WorkerServer started on port ${assignment.assignedPort}")

      HeartbeatManager.start(updatedWorkerInfo, masterClient)

      // ---------------------------------------------------------
      // Sampling
      // ---------------------------------------------------------
      val samples = common.Sampling.uniformEveryN(conf.inputPaths, everyN = 1000)
      println(s"➡️  collected ${samples.size} sample keys")

      FaultInjector.checkAndCrash("after-sampling")

      // ---------------------------------------------------------
      // Splitters creation
      // ---------------------------------------------------------
      masterClient.sendSamples(samples)
      println(s"✅  Sampling phase complete")

      // ---------------------------------------------------------
      // Load and Sort
      // ---------------------------------------------------------
      val allRecords: Vector[Array[Byte]] =
        conf.inputPaths.flatMap(path => readAll(path)).toVector

      println(s"📦 Loaded total ${allRecords.size} records")

      // Parallel sorting
      val sorted = parallelSort(allRecords, numThreads = 4)
      println("🔑 Local sorting completed")

      FaultInjector.checkAndCrash("after-sort")

      // ---------------------------------------------------------
      // Partitioning
      // ---------------------------------------------------------
      val splitterKeys: Array[Array[Byte]] = WorkerState.getSplitters
      println(s"🔑 Loaded ${splitterKeys.length} splitters from PartitionPlan")

      def findPartition(key: Array[Byte]): Int = {
        var idx = 0
        while (idx < splitterKeys.length &&
                RecordIO.compareKeys(splitterKeys(idx), key) < 0) {
          idx += 1
        }
        idx
      }

      val partitioned =
        sorted.groupBy(rec => findPartition(extractKey(rec)))

      println(s"🧩 Partitioning complete → partitions=${partitioned.size}")

      FaultInjector.checkAndCrash("after-partition")

      // ---------------------------------------------------------
      // Shuffle
      // ---------------------------------------------------------
      val workerAddresses = WorkerState.getWorkerAddresses.getOrElse {
        throw new RuntimeException("Worker addresses not available")
      }
      
      def sendPartitionWithRetry(
        originalTarget: Int,
        partitionId: Int,
        records: Seq[Array[Byte]],
        workerAddresses: Map[Int, (String, Int)],
        maxRetries: Int = 5
      ): Boolean = {
        
        var attempt = 0
        
        while (attempt < maxRetries) {              
          try {
            val (targetIp, targetPort) = workerAddresses(originalTarget)
            println(s"  Attempt ${attempt+1}/$maxRetries: p$partitionId → worker#$originalTarget ($targetIp:$targetPort)")
            
            val channel = ManagedChannelBuilder
              .forAddress(targetIp, targetPort)
              .usePlaintext()
              .build()
            
            val stub = WorkerServiceGrpc.stub(channel)
            val ackPromise = scala.concurrent.Promise[Unit]()
            
            val responseObserver = new StreamObserver[Ack] {
              override def onNext(v: Ack): Unit =
                println(s"    ✓ ACK from worker#$originalTarget: ${v.msg}")
              
              override def onError(t: Throwable): Unit = {
                println(s"    ✗ Error: ${t.getMessage}")
                ackPromise.failure(t)
              }
              
              override def onCompleted(): Unit = {
                println(s"    ✓ Completed p$partitionId")
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
            Await.result(ackPromise.future, 30.seconds)
            channel.shutdown()
            
            println(s"  ✅ p$partitionId sent successfully")
            return true
            
          } catch {
            case e: Exception =>
              attempt += 1
              
              if (attempt < maxRetries) {
                val backoff = 3000 * attempt  // 3s, 6s, 9s, 12s, 15s
                println(s"  ⚠️ Send failed, retry after ${backoff}ms: ${e.getMessage}")
                Thread.sleep(backoff)
              } else {
                Console.err.println(s"  ❌ Failed to send p$partitionId after $maxRetries attempts")
                Console.err.println(s"  ℹ️  Will report partial completion to Master")
                return false
              }
          }
        }

        false
      }

      println("-------------------------------------------------------")
      println("     🚚 Starting Shuffle: worker → worker (PARALLEL)")
      println("-------------------------------------------------------")

      try {      
        val maxParallel = 4

        // Partition을 4개씩 묶어서 처리
        val batches = partitioned.toSeq.grouped(maxParallel).toSeq
        
        println(s"  📦 Total ${partitioned.size} partitions in ${batches.size} batches")
        
        batches.zipWithIndex.foreach { case (batch, batchIdx) =>
          println(s"  🔄 Batch ${batchIdx + 1}/${batches.size}: partitions ${batch.map(_._1).mkString(", ")}")

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
          
          // 이번 batch 완료 대기
          val results = Await.result(Future.sequence(batchFutures), 120.seconds)
          val (successes, failures) = results.partition(_._2)

          println(s"  ✅ Batch ${batchIdx + 1}: ${successes.size} sent, ${failures.size} failed")
          if (failures.nonEmpty) {
            println(s"  ⚠️  Failed partitions: ${failures.map(_._1).mkString(", ")} (will recover later)")
          }
        }

      } catch {
        case e: Exception =>
          Console.err.println(s"⚠️ Shuffle encountered errors: ${e.getMessage}")
          e.printStackTrace()
      }

      println("-------------------------------------------------------")
      println("       🎉 Shuffle Completed (with possible failures)")
      println("-------------------------------------------------------")

      FaultInjector.checkAndCrash("after-shuffle")
      
      println("Shuffle completed, reporting to Master...")

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
      WorkerState.reportShuffleComplete()

      println("Shuffle report sent to Master")
      println("⏳ Waiting for finalize command from Master...")

      FaultInjector.checkAndCrash("before-finalize")

      WorkerState.awaitFinalizeComplete()

      HeartbeatManager.stop()
      masterClient.shutdown()
      println("✅ Worker completed successfully")    
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
   * Sender checkpoint 저장 (Atomic write)
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
    
    println(s"  💾 Checkpointed sent_p${partitionId}: ${records.size} records")
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
}
