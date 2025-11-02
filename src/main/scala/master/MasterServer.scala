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
  override def sendSamples(
                            responseObserver: StreamObserver[Splitters]
                          ): StreamObserver[Sample] = {
    // TODO: Week 4 implementation
    new StreamObserver[Sample] {
      override def onNext(sample: Sample): Unit = {
        println(s"Sample received: ${sample.key.size()} bytes")
      }

      override def onError(t: Throwable): Unit = {
        println(s"Error receiving samples: ${t.getMessage}")
      }

      override def onCompleted(): Unit = {
        println("All samples received")
        // TODO: Calculate splitters
        responseObserver.onNext(Splitters(Seq.empty))
        responseObserver.onCompleted()
      }
    }
  }
}