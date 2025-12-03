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
  private val failedWorkers = mutable.Set[Int]()
  private var initialWorkerCount = 0

  def init(n: Int): Unit = synchronized {
    initialWorkerCount = n
    completedWorkers.clear()
    mergeCompletedWorkers.clear()
    failedWorkers.clear()
    println(s"[ShuffleTracker] Initialized for $n workers")
  }

  def markWorkerFailed(workerId: Int): Unit = synchronized {
    failedWorkers += workerId
    println(s"[ShuffleTracker] Worker $workerId FAILED")
    println(s"  Active: ${initialWorkerCount - failedWorkers.size}/$initialWorkerCount")
  }

  def markShuffleComplete(workerId: Int): Unit = synchronized {
    completedWorkers += workerId
    val expected = initialWorkerCount - failedWorkers.size
    println(s"[ShuffleTracker] Worker $workerId shuffle complete (${completedWorkers.size}/$expected)")

    if (isAllShuffleComplete) {
      println("\n" + "=" * 60)
      println("All ALIVE workers completed shuffle phase!")
      println("=" * 60 + "\n")
    }
  }

  def markMergeComplete(workerId: Int): Unit = synchronized {
    mergeCompletedWorkers += workerId
    val expected = initialWorkerCount - failedWorkers.size

    println(s"[ShuffleTracker] Worker $workerId merge complete (${mergeCompletedWorkers.size}/$expected)")

    if (isAllMergeComplete) {
      println("\n" + "=" * 60)
      println("ALL DONE! Distributed sorting complete!")
      println("=" * 60)
      printFinalReport()
    }
  }

  def isAllShuffleComplete: Boolean = {
    val expected = initialWorkerCount - failedWorkers.size
    completedWorkers.size >= expected
  }

  def isAllMergeComplete: Boolean = {
    val expected = initialWorkerCount - failedWorkers.size
    mergeCompletedWorkers.size >= expected
  }

  private def printFinalReport(): Unit = {
    val alive = initialWorkerCount - failedWorkers.size
    println("\nFinal Report:")
    println(s"  Initial workers: $initialWorkerCount")
    println(s"  Failed workers: ${failedWorkers.size}")
    println(s"  Alive workers: $alive")
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

  /* ---------------- Partition Ownership (Week7 Step2) ---------------- */
  private var partitionOwners: Map[Int, Int] = Map.empty

  private def recordPartitionOwners(): Unit = {
    partitionOwners = Map.empty // 추후 Step3 완료시 사용
    println(s"[Master] Partition owners recorded: $partitionOwners")
  }

  private def handleMergeFailure(workerId: Int): Unit = {
    println(s"[Master] handleMergeFailure(): Worker $workerId FAILED during merge")

    val orphaned = partitionOwners.filter(_._2 == workerId).keys.toSet
    if (orphaned.isEmpty) {
      println(s"[Master] No orphaned partitions for worker $workerId")
      return
    }

    println(s"[Master] Orphaned partitions: $orphaned")

    val aliveWorkers = registry.getAliveWorkers
    if (aliveWorkers.isEmpty) {
      Console.err.println("[Master] ERROR: No alive workers remain!")
      return
    }

    val reassigned = orphaned.toSeq.zipWithIndex.map { case (pid, idx) =>
      val newOwner = aliveWorkers(idx % aliveWorkers.size).id
      pid -> newOwner
    }.toMap

    println(s"[Master] Reassignment map: $reassigned")

    partitionOwners = partitionOwners ++ reassigned
  }

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
          Thread.sleep(30000)

          registry.pruneDeadWorkers() { deadId =>
            println(s"[Master] DEAD worker detected → $deadId")
            ShuffleTracker.markWorkerFailed(deadId)
            handleMergeFailure(deadId)
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

  private def expectedWorkers: Int = sampling.expectedWorkers


  /* ---------------- Worker Registration ---------------- */
  override def registerWorker(request: WorkerInfo): Future[WorkerAssignment] = Future {
    val assignment = registry.register(request)

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

          val wAddrs =
            registry.getAllWorkers.map(w => WorkerAddress(w.id, w.workerInfo.ip, w.workerInfo.port))

          val plan = PartitionPlanner.createPlan(split, expectedWorkers, wAddrs)

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
  override def reportShuffleComplete(status: WorkerStatus): Future[Ack] = Future {
    println(s"[Master] Worker ${status.workerId} reported shuffle complete")
    ShuffleTracker.markShuffleComplete(status.workerId)

    if (ShuffleTracker.isAllShuffleComplete) {
      println("[Master] All shuffle complete — triggering finalize")
      // recordPartitionOwners()  // Step3에서 사용
      triggerFinalizePhase()
    }

    Ack(ok = true, msg = "Shuffle completion recorded")
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

