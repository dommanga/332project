package master

import io.grpc.{Server, ServerBuilder, ManagedChannelBuilder}
import rpc.sort.{
  MasterServiceGrpc, WorkerInfo, WorkerAssignment, Ack, 
  Sample, Splitters, PartitionPlan, WorkerServiceGrpc, WorkerStatus, TaskId
}
import master.PartitionPlanner
import scala.concurrent.{ExecutionContext, Future}
import scala.collection.mutable
import io.grpc.stub.StreamObserver

object MasterServer {
  def main(args: Array[String]): Unit = {
    if (args.length < 1) {
      println("Usage: master <num_workers>")
      System.exit(1)
    }

    val numWorkers = args(0).toInt
    val port = 5000

    // Start gRPC server
    val server = new MasterServer(port, numWorkers)
    server.start()

    // Print master address
    val localIP = getLocalIP()
    println("=" * 60)
    println(s"   Master Server Started")
    println(s"   Address: $localIP:$port")
    println(s"   Expected Workers: $numWorkers")
    println("=" * 60)

    // Wait for shutdown
    server.blockUntilShutdown()
  }

  // Get local IP address
  private def getLocalIP(): String = {
    import java.net.{InetAddress, NetworkInterface}
    import scala.jdk.CollectionConverters._

    NetworkInterface.getNetworkInterfaces.asScala
      .flatMap(_.getInetAddresses.asScala)
      .find(addr => !addr.isLoopbackAddress && addr.getAddress.length == 4)
      .map(_.getHostAddress)
      .getOrElse("127.0.0.1")
  }
}

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
    println(s"[ShuffleTracker] Worker $workerId shuffle complete " +
      s"(${completedWorkers.size}/$totalWorkers)")

    if (isAllShuffleComplete) {
      println("\n" + "=" * 60)
      println("All workers completed shuffle phase!")
      println("=" * 60 + "\n")
    }
  }

  def markMergeComplete(workerId: Int): Unit = synchronized {
    mergeCompletedWorkers += workerId
    println(s"[ShuffleTracker] Worker $workerId merge complete " +
      s"(${mergeCompletedWorkers.size}/$totalWorkers)")

    if (isAllMergeComplete) {
      println("\n" + "=" * 60)
      println("ALL DONE! Distributed sorting complete!")
      println("=" * 60)
      printFinalReport()
    }
  }

  def isAllShuffleComplete: Boolean = completedWorkers.size == totalWorkers
  def isAllMergeComplete: Boolean = mergeCompletedWorkers.size == totalWorkers

  private def printFinalReport(): Unit = {
    println("\nFinal Report:")
    println(s"  Total workers: $totalWorkers")
    println(s"  Shuffle completed: ${completedWorkers.size}")
    println(s"  Merge completed: ${mergeCompletedWorkers.size}")
    println("\nCheck output files in each worker's output directory:")
    println("  partition.0, partition.1, partition.2, ...")
  }
}

class MasterServer(port: Int, expectedWorkers: Int) {
  private var server: Server = _
  private implicit val ec: ExecutionContext = ExecutionContext.global

  private val registry = new WorkerRegistry()
  private val sampling = new SamplingCoordinator(expectedWorkers)
  private val serviceImpl = new MasterServiceImpl(registry, sampling)


  def start(): Unit = {
    server = ServerBuilder
      .forPort(port)
      .addService(MasterServiceGrpc.bindService(serviceImpl, ec))
      .build()
      .start()

    println(s"Server ready. Waiting for workers...\n")

    val pruneThread = new Thread {
      override def run(): Unit = {
        while (!Thread.interrupted()) {
          Thread.sleep(30000)
          registry.pruneDeadWorkers()
        }
      }
    }
    pruneThread.setDaemon(true)
    pruneThread.start()

    sys.addShutdownHook {
      println("\nShutting down Master...")
      stop()
    }
  }

  def stop(): Unit = {
    if (server != null) {
      server.shutdown()
    }
  }

  def blockUntilShutdown(): Unit = {
    if (server != null) {
      server.awaitTermination()
    }
  }
}

// gRPC service implementation
class MasterServiceImpl(
  registry: WorkerRegistry, 
  sampling: SamplingCoordinator
)(implicit ec: ExecutionContext)
  extends MasterServiceGrpc.MasterService {

  private val nextSamplesWorker = new java.util.concurrent.atomic.AtomicInteger(0)
  @volatile private var planBroadcasted = false 

  private def expectedWorkers: Int = sampling.expectedWorkers


  override def registerWorker(request: WorkerInfo): Future[WorkerAssignment] = {
    Future {
      val assignment = registry.register(request)

      // 전체 Worker 연결 체크
      if (registry.size == expectedWorkers) {
        println("\nAll workers connected!")
        println("Worker ordering:")
        registry.getAllWorkers.sortBy(_.id).foreach { w =>
          val name = if (w.workerInfo.id.nonEmpty) w.workerInfo.id else w.workerInfo.ip
          println(s"  ${w.id + 1}. $name")
        }
      } else {
        println(s"Waiting for ${sampling.expectedWorkers - registry.size} more workers...")
      }

      assignment
    }
  }

  override def heartbeat(request: WorkerInfo): Future[Ack] = {
    Future {
      registry.updateHeartbeat(request)
      val who = if (request.id.nonEmpty) request.id else s"${request.ip}:${request.port}"
      println(s"Heartbeat received from $who")
      Ack(ok = true, msg = "Heartbeat received")
    }
  }

  override def sendSamples(responseObserver: StreamObserver[Splitters]): StreamObserver[Sample] = {
    val workerId: Int = nextSamplesWorker.getAndIncrement()

    new StreamObserver[Sample] {
      override def onNext(sample: Sample): Unit = {
        // 샘플의 key 바이트 복사해서 수집기에 전달
        val arr: Array[Byte] = sample.key.toByteArray
        sampling.submit(workerId, arr)
      }

      override def onError(t: Throwable): Unit = {
        Console.err.println(s"[sendSamples] stream error from worker#$workerId: ${t.getMessage}")
      }

      override def onCompleted(): Unit = {
        // 이 워커의 샘플 스트림 종료 표시
        sampling.complete(workerId)

        // (간단 대기) 모든 워커 스트림이 끝나 스플리터가 준비될 때까지 최대 30초 대기
        val deadlineNanos = System.nanoTime() + 30_000_000_000L // 30s
        while (!sampling.isReady && System.nanoTime() < deadlineNanos) {
          Thread.sleep(50)
        }

        // 준비된 splitters를 응답으로 전송
        val splittersArr = sampling.splitters
        val resp = Splitters(
          key = splittersArr.map(com.google.protobuf.ByteString.copyFrom).toIndexedSeq
        )

        responseObserver.onNext(resp)
        responseObserver.onCompleted()

        println(s"[sendSamples] worker#$workerId completed. " +
          s"splitters_ready=${splittersArr.nonEmpty} count=${splittersArr.length}")
                
        if (sampling.isReady && !planBroadcasted){
          planBroadcasted = true
          val plan = PartitionPlanner.createPlan(splittersArr, expectedWorkers)
          println(s"[Master] Broadcasting PartitionPlan to $expectedWorkers workers...")

          registry.getAllWorkers.foreach { w =>
            val target = s"${w.workerInfo.ip}:${w.workerInfo.port}" 
            try {
              val ch = io.grpc.ManagedChannelBuilder.forTarget(target).usePlaintext().build()
              val stub = WorkerServiceGrpc.blockingStub(ch)
              val ack = stub.setPartitionPlan(plan)
              println(s"  → worker#${w.id} (${w.workerInfo.ip}) ack=${ack.ok}")

              val shuffleAck = stub.startShuffle(TaskId("task-001"))
              println(s"  → worker#${w.id} shuffle started: ${shuffleAck.msg}")

              ch.shutdown()
            } catch {
              case e: Exception => 
                println(s"  ✗ Failed to send plan to ${w.workerInfo.ip}: ${e.getMessage}")
            }
          }

          ShuffleTracker.init(expectedWorkers)
        }
      }
    }
  }

  override def reportShuffleComplete(status: WorkerStatus): Future[Ack] = {
    Future {
      println(s"[Master] Worker ${status.workerId} reported shuffle complete")
      ShuffleTracker.markShuffleComplete(status.workerId)

      if (ShuffleTracker.isAllShuffleComplete) {
        triggerFinalizePhase()
      }

      Ack(ok = true, msg = "Shuffle completion noted")
    }
  }

  override def reportMergeComplete(status: WorkerStatus): Future[Ack] = {
    Future {
      println(s"[Master] Worker ${status.workerId} reported merge complete")
      ShuffleTracker.markMergeComplete(status.workerId)
      Ack(ok = true, msg = "Merge completion noted")
    }
  }

  private def triggerFinalizePhase(): Unit = {
    println("\n[Master] Triggering finalize phase on all workers...")

    registry.getAllWorkers.foreach { w =>
      val target = s"${w.workerInfo.ip}:${w.workerInfo.port}"
      try {
        val ch = io.grpc.ManagedChannelBuilder.forTarget(target).usePlaintext().build()
        val stub = WorkerServiceGrpc.blockingStub(ch)
        val ack = stub.finalizePartitions(TaskId("task-001"))
        println(s"  ✓ Worker ${w.id} finalize started: ${ack.msg}")
        ch.shutdown()
      } catch {
        case e: Exception =>
          println(s"  ✗ Failed to send finalize to worker ${w.id}: ${e.getMessage}")
      }
    }
  }
}