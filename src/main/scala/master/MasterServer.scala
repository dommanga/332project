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
    val port = 5100

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
          Thread.sleep(3000)

          registry.pruneDeadWorkers(timeoutSeconds = 10) { deadId =>
            println(s"[Master] DEAD worker detected → $deadId")
            
            serviceImpl.handleWorkerFailure(deadId)
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

  object PlanStore {
    @volatile private var latestPlan: Option[PartitionPlan] = None
    
    def set(plan: PartitionPlan): Unit = {
      latestPlan = Some(plan)
      println(s"[PlanStore] Cached PartitionPlan with ${plan.ranges.size} ranges")
    }
    
    def get: Option[PartitionPlan] = latestPlan
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
      
      // ===== PartitionPlan 재전송 + Recovery trigger(Shuffle Complete - automatic missing detection) =====
      Future {
        Thread.sleep(2000)

        PlanStore.get match {
          case Some(plan) =>
            val (ip, port) = (request.ip, assignment.assignedPort)
            val channel = ManagedChannelBuilder.forAddress(ip, port)
              .usePlaintext()
              .build()
            val stub = WorkerServiceGrpc.blockingStub(channel)
            
            // PartitionPlan 재전송
            stub.setPartitionPlan(plan)
            println(s"[Master] ✅ Resent PartitionPlan to Worker ${assignment.workerId}")

            ShuffleTracker.markShuffleComplete(assignment.workerId)

            if (ShuffleTracker.isAllShuffleComplete) {
              println("[Master] All alive workers completed shuffle (including recovered)")
              triggerFinalizePhase()
            }
            
            channel.shutdown()
            
          case None =>
            Console.err.println("[Master] ⚠️ No cached PartitionPlan to resend!")
        }
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

        val limit = System.nanoTime() + 60_000_000_000L
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
          PlanStore.set(plan)

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

  def handleWorkerFailure(workerId: Int): Unit = {
    println(s"[Master] handleWorkerFailure(): Worker $workerId FAILED")

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
      broadcastShutdown()

      println("[Master] 🎉 All work complete! Shutting down in 3 seconds...")
      Future {
        Thread.sleep(3000)  // Worker들의 마지막 응답 전송 대기
        println("[Master] Goodbye!")
        System.exit(0)
      }
    }

    Ack(ok = true, msg = "Merge completion noted")
  }

  private def initializeShufflePlan(numWorkers: Int): Unit = {
    val numPartitions = numWorkers * 4
    val partitionsPerWorker = (numPartitions + numWorkers - 1) / numWorkers

    (0 until numPartitions).foreach { pid =>
      shufflePlan(pid) = (0 until numWorkers).toSet
      val targetWorker = pid / partitionsPerWorker
      partitionOwners = partitionOwners.updated(pid, targetWorker)
    }
    println(s"[Master] Initialized: $numPartitions partitions, $numWorkers workers")
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

  private def broadcastShutdown(): Unit = {
    println("[Master] Broadcasting shutdown command to all workers...")
    
    registry.getAllWorkers.foreach { w =>
      try {
        val ch = ManagedChannelBuilder
          .forAddress(w.workerInfo.ip, w.workerInfo.port)
          .usePlaintext()
          .build()
        
        val stub = WorkerServiceGrpc.blockingStub(ch)
        val ack = stub.shutdown(TaskId("shutdown"))
        println(s"  Worker${w.id} shutdown = ${ack.msg}")
        
        ch.shutdown()
      } catch {
        case e: Exception =>
          Console.err.println(s"  Worker${w.id} shutdown failed: ${e.getMessage}")
      }
    }
  }
}

