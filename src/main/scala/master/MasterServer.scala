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
    if (args.length != 1) {
      println("Usage: master <num_workers>")
      System.exit(1)
    }

    val numWorkers = args(0).toInt
    val server = new MasterServer(0, numWorkers)

    val actualPort = server.start()

    val localIP = getLocalIP()
    println(s"$localIP:$actualPort")

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
 *  ShuffleTracker
 * ================================================================ */
object ShuffleTracker {
  private val completedWorkers = mutable.Set[Int]()
  private val mergeCompletedWorkers = mutable.Set[Int]()
  private var totalWorkers = 0

  private var timerStarted    = false
  private var startTimeNanos: Long = 0L
  private var endTimeNanos:   Long = 0L

  def startGlobalTimer(): Boolean = synchronized {
    if (!timerStarted) {
      timerStarted = true
      startTimeNanos = System.nanoTime()
      endTimeNanos = 0L
      true
    } else {
      false
    }
  }

  def init(n: Int): Unit = synchronized {
    totalWorkers = n
    completedWorkers.clear()
    mergeCompletedWorkers.clear()
  }

  def markShuffleComplete(workerId: Int): Unit = synchronized {
    completedWorkers += workerId
    println(s"✅ Worker $workerId shuffle complete (${completedWorkers.size}/$totalWorkers)")

    if (isAllShuffleComplete) {
      println("\n" + "=" * 60)
      println("All workers completed shuffle phase!")
      println("=" * 60 + "\n")
    }
  }

  def markMergeComplete(workerId: Int): Unit = synchronized {
    mergeCompletedWorkers += workerId
    println(s"✅ Worker $workerId merge complete (${mergeCompletedWorkers.size}/$totalWorkers)")

    if (isAllMergeComplete) {
      endTimeNanos = System.nanoTime()

      println("\n" + "=" * 60)
      println("🎉 Distributed sorting complete!")
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
    println("\n===== FINAL EXECUTION REPORT =====")
    println(s"Total workers: $totalWorkers")
    println(s"Shuffle completed: ${completedWorkers.size}")
    println(s"Merge completed: ${mergeCompletedWorkers.size}")

    if (timerStarted && startTimeNanos != 0L && endTimeNanos != 0L) {
      val seconds = (endTimeNanos - startTimeNanos) / 1e9
      println(f"⏱️  Execution time: $seconds%.2f s")
    } else {
      println("⏱️  Timer not available")
    }
    println("=================================")
  }
}

/* ================================================================
 *  MasterServer Implementation
 * ================================================================ */
class MasterServer(port: Int, expectedWorkers: Int) {
  private var server: Server = _
  private implicit val ec: ExecutionContext = ExecutionContext.global

  private val registry = new WorkerRegistry(expectedWorkers)
  private val sampling = new SamplingCoordinator(expectedWorkers)
  private val serviceImpl = new MasterServiceImpl(registry, sampling)

  private var boundPort: Int = _

  def start(): Int = {
    server = ServerBuilder
      .forPort(port)
      .addService(MasterServiceGrpc.bindService(serviceImpl, ec))
      .build()
      .start()

    boundPort = server.getPort

    // Worker timeout thread
    val pruneThread = new Thread {
      override def run(): Unit = {
        while (!Thread.interrupted()) {
          Thread.sleep(2000)

          registry.pruneDeadWorkers(timeoutSeconds = 5) { deadId =>
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
    boundPort
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

  private val shufflePlan = mutable.Map[Int, Set[Int]]()
  private val shuffleCompletions = mutable.Map[(Int, Int, Int), Boolean]()

  private def expectedWorkers: Int = sampling.expectedWorkers

  object PlanStore {
    @volatile private var latestPlan: Option[PartitionPlan] = None
    
    def set(plan: PartitionPlan): Unit = {
      latestPlan = Some(plan)
    }
    
    def get: Option[PartitionPlan] = latestPlan
  }

  /* ---------------- Worker Registration ---------------- */
  override def registerWorker(request: WorkerInfo): Future[WorkerAssignment] = Future {
    val assignment = registry.register(request)

    val orphaned = partitionOwners.filter(_._2 == -1).keys.toSet

    if (orphaned.nonEmpty && assignment.workerId < expectedWorkers) {
      println(s"🎉 Worker ${assignment.workerId} rejoined!")
      println(s"📦 Assigning recovery partitions: $orphaned")
      
      orphaned.foreach { pid =>
        partitionOwners = partitionOwners.updated(pid, assignment.workerId)
      }
      
      Future {
        Thread.sleep(2000)

        PlanStore.get match {
          case Some(plan) =>
            val (ip, port) = (request.ip, assignment.assignedPort)
            val channel = ManagedChannelBuilder.forAddress(ip, port)
              .usePlaintext()
              .build()
            val stub = WorkerServiceGrpc.blockingStub(channel)
            
            stub.setPartitionPlan(plan)
            println(s"✅ Resent PartitionPlan to Worker ${assignment.workerId}")

            ShuffleTracker.markShuffleComplete(assignment.workerId)

            if (ShuffleTracker.isAllShuffleComplete) {
              println("🔧 All workers completed shuffle, triggering finalize")
              triggerFinalizePhase()
            }
            
            channel.shutdown()
            
          case None =>
            Console.err.println("⚠️ No cached PartitionPlan to resend!")
        }
      }
    }

    if (registry.size == expectedWorkers) {
      if (ShuffleTracker.startGlobalTimer()) {
        val workerIPs = registry.getAllWorkers.sortBy(_.id).map(_.workerInfo.ip)
        println(workerIPs.mkString(", "))
        println("\n📌 All workers registered!\n")
      }
    }
    assignment
  }

  /* ---------------- Heartbeat ---------------- */
  override def heartbeat(request: WorkerInfo): Future[Ack] = Future {
    registry.updateHeartbeat(request)
    Ack(ok = true, msg = "OK")
  }

  /* ---------------- Sampling ---------------- */
  override def sendSamples(responseObserver: StreamObserver[Splitters]): StreamObserver[Sample] = {
    val workerId = nextSamplesWorker.getAndIncrement()

    new StreamObserver[Sample] {
      override def onNext(sample: Sample): Unit =
        sampling.submit(workerId, sample.key.toByteArray)

      override def onError(t: Throwable): Unit =
        Console.err.println(s"❌ Sampling error from Worker $workerId: ${t.getMessage}")

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

          println("📋 Broadcasting PartitionPlan to workers...")

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
          println("✅ Shuffle phase started\n")
        }
      }
    }
  }

  def handleWorkerFailure(workerId: Int): Unit = {
    val orphaned = partitionOwners.filter(_._2 == workerId).keys.toSet
    if (orphaned.isEmpty) {
      return
    }

    println(s"⚠️  Worker $workerId failed - partitions $orphaned orphaned")
    
    orphaned.foreach { pid =>
      partitionOwners = partitionOwners.updated(pid, -1)
    }
    
    println(s"ℹ️  Please restart Worker $workerId to recover")
  }

  /* ---------------- Report Shuffle ---------------- */
  override def reportShuffleComplete(report: ShuffleCompletionReport): Future[Ack] = Future {
    val workerId = report.workerId
    
    report.sendRecords.foreach { record =>
      val key = (record.senderId, record.partitionId, record.targetWorkerId)
      shuffleCompletions(key) = record.success
    }
    
    ShuffleTracker.markShuffleComplete(workerId)
    
    if (ShuffleTracker.isAllShuffleComplete) {
      println("🔧 All workers completed shuffle, triggering finalize")
      triggerFinalizePhase()
    }
    
    Ack(ok = true, msg = "Recorded")
  }

  override def queryPartitionSenders(query: PartitionSendersQuery): Future[PartitionSendersResponse] = Future {
    val pid = query.partitionId
    val senders = shufflePlan.getOrElse(pid, Set.empty)
    
    PartitionSendersResponse(
      partitionId = pid,
      senderIds = senders.toSeq
    )
  }

  /* ---------------- Report Merge ---------------- */
  override def reportMergeComplete(status: WorkerStatus): Future[Ack] = Future {
    ShuffleTracker.markMergeComplete(status.workerId)

    if (ShuffleTracker.isAllMergeComplete) {
      broadcastShutdown()

      println("\n🎉 All work complete! Shutting down in 3 seconds...")
      Future {
        Thread.sleep(3000)
        println("👋 Goodbye!")
        System.exit(0)
      }
    }

    Ack(ok = true, msg = "Recorded")
  }

  private def initializeShufflePlan(numWorkers: Int): Unit = {
    val numPartitions = numWorkers * 4
    val partitionsPerWorker = (numPartitions + numWorkers - 1) / numWorkers

    (0 until numPartitions).foreach { pid =>
      shufflePlan(pid) = (0 until numWorkers).toSet
      val targetWorker = pid / partitionsPerWorker
      partitionOwners = partitionOwners.updated(pid, targetWorker)
    }
  }

  /* ---------------- Finalize ---------------- */
  private def triggerFinalizePhase(): Unit = {
    println("🔧 Triggering finalize phase...\n")

    registry.getAllWorkers.foreach { w =>
      val ch = ManagedChannelBuilder.forAddress(w.workerInfo.ip, w.workerInfo.port)
        .usePlaintext()
        .build()

      val stub = WorkerServiceGrpc.blockingStub(ch)
      stub.finalizePartitions(TaskId("task-001"))

      ch.shutdown()
    }
  }

  private def broadcastShutdown(): Unit = {
    println("🛑 Broadcasting shutdown to workers...")
    
    registry.getAllWorkers.foreach { w =>
      try {
        val ch = ManagedChannelBuilder
          .forAddress(w.workerInfo.ip, w.workerInfo.port)
          .usePlaintext()
          .build()
        
        val stub = WorkerServiceGrpc.blockingStub(ch)
        stub.shutdown(TaskId("shutdown"))
        
        ch.shutdown()
      } catch {
        case e: Exception =>
          Console.err.println(s"⚠️ Worker ${w.id} shutdown failed: ${e.getMessage}")
      }
    }
  }
}