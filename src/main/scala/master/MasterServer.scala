package master

import io.grpc.{Server, ServerBuilder}
import rpc.sort.{MasterServiceGrpc, WorkerInfo, WorkerAssignment, Ack, Sample, Splitters}
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
    println(s"Master listening on $localIP:$port")
    println(s"Waiting for $numWorkers workers to connect...")

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
  private val serviceImpl = new MasterServiceImpl(expectedWorkers)

  def start(): Unit = {
    server = ServerBuilder
      .forPort(port)
      .addService(MasterServiceGrpc.bindService(serviceImpl, ExecutionContext.global))
      .build()
      .start()

    println(s"Server started on port $port")

    sys.addShutdownHook {
      println("Shutting down gRPC server...")
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
class MasterServiceImpl(expectedWorkers: Int)(implicit ec: ExecutionContext)
  extends MasterServiceGrpc.MasterService {

  private val workers = mutable.ListBuffer[WorkerInfo]()
  private var nextWorkerId = 0
  private val sampling = new SamplingCoordinator(expectedWorkers)
  private val nextSamplesWorker = new java.util.concurrent.atomic.AtomicInteger(0)

  override def registerWorker(request: WorkerInfo): Future[WorkerAssignment] = {
    synchronized {
      val workerId = nextWorkerId
      nextWorkerId += 1

      workers += request

      val displayName = if (request.id.nonEmpty) request.id else request.ip
      println(s"Worker registered: $displayName -> Worker #$workerId")

      // Check if all workers connected
      if (workers.size == expectedWorkers) {
        println("\nAll workers connected!")
        println("Worker ordering:")
        workers.zipWithIndex.foreach { case (w, idx) =>
          val name = if (w.id.nonEmpty) w.id else w.ip
          println(s"  ${idx + 1}. $name")
        }
      } else {
        println(s"Waiting for ${expectedWorkers - workers.size} more workers...")
      }

      // Assign partitions (dummy for now, Week 4+)
      val partitions = (workerId * 3 until (workerId + 1) * 3).toSeq

      // (선택) 나중에 peer-IP 매칭용으로 저장할 수도 있음
      // workerIdByIp.update(request.ip, workerId)
      Future.successful(
        WorkerAssignment(
          success = true,
          message = "Registration successful",
          workerId = workerId,
          partitionIds = partitions
        )
      )
    }
  }

  // Week 4: Heartbeat (dummy)
  override def heartbeat(request: WorkerInfo): Future[Ack] = {
    // TODO: Week 4 implementation
    println(s"Heartbeat received from ${request.id}")
    Future.successful(Ack(ok = true, msg = "Heartbeat received"))
  }

  // Week 4: SendSamples (dummy)
  override def sendSamples(responseObserver: StreamObserver[Splitters]): StreamObserver[Sample] = {
    val workerId: Int = nextSamplesWorker.getAndIncrement()

    new StreamObserver[Sample] {
      override def onNext(sample: Sample): Unit = {
        // 샘플의 key 바이트 복사해서 수집기에 전달
        val arr: Array[Byte] = sample.key.toByteArray
        sampling.submit(workerId, arr)
      }

      override def onError(t: Throwable): Unit = {
        println(s"[sendSamples] stream error from worker#$workerId: ${t.getMessage}")
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
          splittersArr.map(com.google.protobuf.ByteString.copyFrom).toIndexedSeq
        )

        responseObserver.onNext(resp)
        responseObserver.onCompleted()

        println(s"[sendSamples] worker#$workerId completed. " +
          s"splitters_ready=${splittersArr.nonEmpty} count=${splittersArr.length}")
      }
    }
  }
}