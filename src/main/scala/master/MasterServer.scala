package master

import io.grpc.{Server, ServerBuilder, ManagedChannelBuilder}
import rpc.sort.{
  MasterServiceGrpc, WorkerInfo, WorkerAssignment, Ack, 
  Sample, Splitters, PartitionPlan, WorkerServiceGrpc
}
import master.PartitionPlanner
import scala.concurrent.{ExecutionContext, Future}
import scala.collection.mutable
import io.grpc.stub.StreamObserver

object MockSplitterCalculator extends SplitterCalculator {
  override def calculate(samples: Array[Array[Byte]], numWorkers: Int): Array[Array[Byte]] = {
    println(s"🔧 [MOCK] Calculating ${numWorkers-1} splitters from ${samples.length} samples")
    Array.empty[Array[Byte]]
  }
}

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
      val dummyPartitions = (assignment.workerId * 3 until (assignment.workerId + 1) * 3).toSeq
      val assignmentWithPartitions = assignment.copy(partitionIds = dummyPartitions)

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

      assignmentWithPartitions
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

  // Week 4: SendSamples
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
              ch.shutdown()
            } catch {
              case e: Exception => 
                println(s"  ✗ Failed to send plan to ${w.workerInfo.ip}: ${e.getMessage}")
            }
          }
        }
      }
    }
  }
}