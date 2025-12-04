package master

import io.grpc.{Server, ServerBuilder, ManagedChannelBuilder}
import rpc.sort._
import master.PartitionPlanner

import scala.concurrent.{ExecutionContext, Future}
import scala.collection.mutable
import io.grpc.stub.StreamObserver

/* ================================================================
 *  MasterServer Main
 * ================================================================ */
object MasterServer {
  def main(args: Array[String]): Unit = {
    if (args.length < 1) {
      println("Usage: master <num_workers>")
      System.exit(1)
    }

    val numWorkers = args(0).toInt
    val port = 5000

    val server = new MasterServer(port, numWorkers)
    server.start()

    val localIP = getLocalIP()
    println("=" * 60)
    println(s"   Master Server Started")
    println(s"   Address: $localIP:$port")
    println(s"   Expected Workers: $numWorkers")
    println("=" * 60)

    server.blockUntilShutdown()
  }

  private def getLocalIP(): String = {
    import java.net.NetworkInterface
    import scala.jdk.CollectionConverters._
    NetworkInterface.getNetworkInterfaces.asScala
      .flatMap(_.getInetAddresses.asScala)
      .find(addr => !addr.isLoopbackAddress && addr.getAddress.length == 4)
      .map(_.getHostAddress)
      .getOrElse("127.0.0.1")
  }
}


/* ================================================================
 *  ShuffleTracker (Week7 버전: Dead Worker 제외)
 * ================================================================ */
object ShuffleTracker {
  private val completedWorkers = mutable.Set[Int]()
  private val mergeCompletedWorkers = mutable.Set[Int]()
  private var totalWorkers = 0

  def init(n: Int): Unit = synchronized {
    totalWorkers = n
    completedWorkers.clear()
    mergeCompletedWorkers.clear()
    println(s"[ShuffleTracker] Initialized for $n workers")
  }

  def markShuffleComplete(workerId: Int): Unit = synchronized {
    completedWorkers += workerId
    println(s"[ShuffleTracker] Worker $workerId shuffle complete (${completedWorkers.size}/$totalWorkers)")

    if (isAllShuffleComplete) {
      println("\n" + "=" * 60)
      println("All ALIVE workers completed shuffle phase!")
      println("=" * 60 + "\n")
    }
  }

  def markMergeComplete(workerId: Int): Unit = synchronized {
    mergeCompletedWorkers += workerId

    println(s"[ShuffleTracker] Worker $workerId merge complete (${mergeCompletedWorkers.size}/$totalWorkers)")

    if (isAllMergeComplete) {
      println("\n" + "=" * 60)
      println("ALL DONE! Distributed sorting complete!")
      println("=" * 60)
      printFinalReport()
    }
  }

  def isAllShuffleComplete: Boolean = {
    completedWorkers.size >= totalWorkers
  }

  def isAllMergeComplete: Boolean = {
    mergeCompletedWorkers.size >= totalWorkers
  }

  private def printFinalReport(): Unit = {
    println("\nFinal Report:")
    println(s"  Initial workers: $totalWorkers")
    println(s"  Shuffle completed: ${completedWorkers.size}")
    println(s"  Merge completed: ${mergeCompletedWorkers.size}")
  }
}

/* ================================================================
 *  MasterServer Implementation (Week7)
 * ================================================================ */
class MasterServer(port: Int, expectedWorkers: Int) {
  private var server: Server = _
  private implicit val ec: ExecutionContext = ExecutionContext.global

  private val registry = new WorkerRegistry()
  private val sampling = new SamplingCoordinator(expectedWorkers)
  private val serviceImpl = new MasterServiceImpl(registry, sampling)

  /* ================================================================
   * MasterServer Lifecycle
   * ================================================================ */
  def start(): Unit = {
    server = ServerBuilder
      .forPort(port)
      .addService(MasterServiceGrpc.bindService(serviceImpl, ec))
      .build()
      .start()

    println("Server ready. Waiting for workers...")

    // Worker timeout thread
    val pruneThread = new Thread {
      override def run(): Unit = {
        while (!Thread.interrupted()) {
          Thread.sleep(5000)

          registry.pruneDeadWorkers(timeoutSeconds = 10) { deadId =>
            println(s"[Master] DEAD worker detected → $deadId")
          }
        }
      }
    }

    pruneThread.setDaemon(true)
    pruneThread.start()

    sys.addShutdownHook {
      println("Shutting down Master...")
      stop()
    }
  }

  def stop(): Unit = server.shutdown()
  def blockUntilShutdown(): Unit = server.awaitTermination()
}

/* ================================================================
 *  MasterServiceImpl
 * ================================================================ */
class MasterServiceImpl(
  registry: WorkerRegistry,
  sampling: SamplingCoordinator
)(implicit ec: ExecutionContext)
  extends MasterServiceGrpc.MasterService {

  private val nextSamplesWorker = new java.util.concurrent.atomic.AtomicInteger(0)
  @volatile private var planBroadcasted = false

  private var partitionOwners: Map[Int, Int] = Map.empty

  private val shufflePlan = mutable.Map[Int, Set[Int]]() // partition_id → Set[sender_ids]
  private val shuffleCompletions = mutable.Map[(Int, Int, Int), Boolean]() // (sender_id, partition_id, receiver_id) → success

  private def expectedWorkers: Int = sampling.expectedWorkers

  /**
   * Worker 재등록 시 recovery 명령 전송
   */
  private def triggerRecovery(
    workerId: Int, 
    port: Int, 
    ip: String, 
    partitionIds: Set[Int]
  ): Unit = {
    try {
      val channel = ManagedChannelBuilder
        .forAddress(ip, port)
        .usePlaintext()
        .build()
      
      val stub = WorkerServiceGrpc.blockingStub(channel)
      val request = RecoveryRequest(partitionIds = partitionIds.toSeq)
      val ack = stub.recoverPartitions(request)
      
      println(s"[Master] ✅ Recovery command sent to Worker $workerId")
      println(s"[Master]    Response: ${ack.msg}")
      
      channel.shutdown()
      
    } catch {
      case e: Exception =>
        Console.err.println(s"[Master] ❌ Failed to send recovery: ${e.getMessage}")
        e.printStackTrace()
    }
  }

  private def handleMergeFailure(workerId: Int): Unit = {
    println(s"[Master] handleMergeFailure(): Worker $workerId FAILED")

    val orphaned = partitionOwners.filter(_._2 == workerId).keys.toSet
    if (orphaned.isEmpty) {
      println(s"[Master] No orphaned partitions for worker $workerId")
      return
    }

    println(s"[Master] Orphaned partitions: $orphaned")
    
    // Pending recovery 상태로 표시
    orphaned.foreach { pid =>
      partitionOwners = partitionOwners.updated(pid, -1)
    }
    
    println(s"[Master] ⚠️ Partitions $orphaned are PENDING RECOVERY")
    println(s"[Master] ℹ️ Please restart Worker $workerId on its original node")
  }

  /* ---------------- Worker Registration ---------------- */
  override def registerWorker(request: WorkerInfo): Future[WorkerAssignment] = Future {
    val assignment = registry.register(request)

    val orphaned = partitionOwners.filter(_._2 == -1).keys.toSet

    if (orphaned.nonEmpty && assignment.workerId < expectedWorkers) {
      println(s"[Master] 🎉 Worker ${assignment.workerId} REJOINED!")
      println(s"[Master] Assigning recovery partitions: $orphaned")
      
      orphaned.foreach { pid =>
        partitionOwners = partitionOwners.updated(pid, assignment.workerId)
      }
      
      // Recovery 명령 전송 (비동기)
      Future {
        Thread.sleep(2000)
        triggerRecovery(assignment.workerId, assignment.assignedPort, request.ip, orphaned)
      }
    }

    if (registry.size == expectedWorkers) {
      println("\nAll workers connected!")
      registry.getAllWorkers.sortBy(_.id).foreach { w =>
        println(s"  ${w.id}: ${w.workerInfo.ip}")
      }
    }
    assignment
  }


  /* ---------------- Heartbeat ---------------- */
  override def heartbeat(request: WorkerInfo): Future[Ack] = Future {
    registry.updateHeartbeat(request)
    println(s"Heartbeat from ${request.id}")
    Ack(ok = true, msg = "Heartbeat OK")
  }


  /* ---------------- Sampling ---------------- */
  override def sendSamples(responseObserver: StreamObserver[Splitters]): StreamObserver[Sample] = {
    val workerId = nextSamplesWorker.getAndIncrement()

    new StreamObserver[Sample] {
      override def onNext(sample: Sample): Unit =
        sampling.submit(workerId, sample.key.toByteArray)

      override def onError(t: Throwable): Unit =
        println(s"[sendSamples] Worker#$workerId error: ${t.getMessage}")

      override def onCompleted(): Unit = {
        sampling.complete(workerId)

        val limit = System.nanoTime() + 30_000_000_000L
        while (!sampling.isReady && System.nanoTime() < limit)
          Thread.sleep(50)

        val split = sampling.splitters

        responseObserver.onNext(
          Splitters(key = split.map(com.google.protobuf.ByteString.copyFrom).toIndexedSeq)
        )
        responseObserver.onCompleted()

        if (sampling.isReady && !planBroadcasted) {
          planBroadcasted = true

          initializeShufflePlan(expectedWorkers)

          val wAddrs =
            registry.getAllWorkers.map(w => WorkerAddress(w.id, w.workerInfo.ip, w.workerInfo.port))

          val plan = PartitionPlanner.createPlan(
            split.toSeq,
            expectedWorkers, 
            wAddrs
          )

          println("[Master] Broadcasting PartitionPlan")

          registry.getAllWorkers.foreach { w =>
            val ch = ManagedChannelBuilder.forAddress(w.workerInfo.ip, w.workerInfo.port)
              .usePlaintext()
              .build()
            val stub = WorkerServiceGrpc.blockingStub(ch)

            stub.setPartitionPlan(plan)
            stub.startShuffle(TaskId("task-001"))

            ch.shutdown()
          }

          ShuffleTracker.init(expectedWorkers)
        }
      }
    }
  }


  /* ---------------- Report Shuffle ---------------- */
  override def reportShuffleComplete(report: ShuffleCompletionReport): Future[Ack] = Future {
    val workerId = report.workerId
    
    println(s"[Master] Shuffle report from Worker $workerId:")
    report.sendRecords.foreach { record =>
      val key = (record.senderId, record.partitionId, record.targetWorkerId)
      shuffleCompletions(key) = record.success
      
      val status = if (record.success) "✅" else "❌"
      println(s"  $status p${record.partitionId} → Worker${record.targetWorkerId}")
    }
    
    ShuffleTracker.markShuffleComplete(workerId)
    
    if (ShuffleTracker.isAllShuffleComplete) {
      println("[Master] All alive workers completed shuffle")
      triggerFinalizePhase()
    }
    
    Ack(ok = true, msg = "Shuffle report recorded")
  }

  override def queryPartitionSenders(query: PartitionSendersQuery): Future[PartitionSendersResponse] = Future {
    val pid = query.partitionId
    val senders = shufflePlan.getOrElse(pid, Set.empty)
    
    println(s"[Master] Query p$pid senders: $senders")
    
    PartitionSendersResponse(
      partitionId = pid,
      senderIds = senders.toSeq
    )
  }


  /* ---------------- Report Merge ---------------- */
  override def reportMergeComplete(status: WorkerStatus): Future[Ack] = Future {
    println(s"[Master] Worker ${status.workerId} reported merge complete")
    ShuffleTracker.markMergeComplete(status.workerId)

    if (ShuffleTracker.isAllMergeComplete) {
      println("[Master] All merge complete — running verification...")
      triggerVerification()
    }

    Ack(ok = true, msg = "Merge completion noted")
  }

  private def initializeShufflePlan(numWorkers: Int): Unit = {
    // 일단 100개 partition 가정 (나중에 동적으로 변경 가능)
    val numPartitions = numWorkers * 4

    (0 until numPartitions).foreach { pid =>
      shufflePlan(pid) = (0 until numWorkers).toSet
      partitionOwners = partitionOwners.updated(pid, pid % numWorkers)
    }
    println(s"[Master] Initialized: $numPartitions partitions, $numWorkers workers")
  }


  /* ---------------- Verification ---------------- */
  private def triggerVerification(): Unit = {
    println("\n" + "=" * 60)
    println("[Master] Running final verification...")
    println("=" * 60)

    try {
      val outputDir = "out"
      val expectedPartitions = expectedWorkers
      val expectedRecords: Long = 1000000L

      val ok = worker.VerifyOutput.runFullVerification(
        outputDir,
        expectedPartitions,
        expectedRecords
      )

      if (ok) {
        println("\n✅ Verification PASSED — All outputs are correct.")
      } else {
        println("\n❌ Verification FAILED — Output incorrect.")
      }

    } catch {
      case e: Exception =>
        println(s"[Master] Verification error: ${e.getMessage}")
    }

    println("=" * 60)
  }


  /* ---------------- Finalize ---------------- */
  private def triggerFinalizePhase(): Unit = {
    println("[Master] Triggering finalize phase...")

    registry.getAllWorkers.foreach { w =>
      val ch = ManagedChannelBuilder.forAddress(w.workerInfo.ip, w.workerInfo.port)
        .usePlaintext()
        .build()

      val stub = WorkerServiceGrpc.blockingStub(ch)
      val ack = stub.finalizePartitions(TaskId("task-001"))
      println(s"  Worker${w.id} finalize = ${ack.msg}")

      ch.shutdown()
    }
  }
}

