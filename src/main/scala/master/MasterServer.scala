package master

import io.grpc.{Server, ServerBuilder}
import rpc.sort.{MasterServiceGrpc, WorkerInfo, WorkerAssignment, Ack, Sample, Splitters}
import scala.concurrent.{ExecutionContext, Future}
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
  private val sampleCollector = new SampleCollectorImpl(expectedWorkers)
  private val splitterCalculator = MockSplitterCalculator

  private val serviceImpl = new MasterServiceImpl(
    registry,
    sampleCollector,
    splitterCalculator
  )

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

class MasterServiceImpl(
                         registry: WorkerRegistry,
                         sampleCollector: SampleCollectorImpl,
                         splitterCalculator: SplitterCalculator
                       )(implicit ec: ExecutionContext) extends MasterServiceGrpc.MasterService {

  override def registerWorker(request: WorkerInfo): Future[WorkerAssignment] = {
    Future {
      registry.register(request)
    }
  }

  override def heartbeat(request: WorkerInfo): Future[Ack] = {
    Future {
      registry.updateHeartbeat(request)
    }
  }

  override def sendSamples(responseObserver: StreamObserver[Splitters]): StreamObserver[Sample] = {

    var workerId: String = null

    new StreamObserver[Sample] {
      override def onNext(sample: Sample): Unit = {
        val keyBytes = sample.key.toByteArray

        if (workerId == null) {
          workerId = "worker-" + System.currentTimeMillis()
        }

        sampleCollector.addSample(workerId, keyBytes)
      }

      override def onError(t: Throwable): Unit = {
        Console.err.println(s"Error receiving samples: ${t.getMessage}")
      }

      override def onCompleted(): Unit = {
        sampleCollector.markWorkerComplete(workerId)

        if (sampleCollector.isAllWorkersComplete) {
          println("All workers completed sampling. Calculating splitters...")
          sampleCollector.printStats()

          val allSamples = sampleCollector.collectAllSamples()
          val numWorkers = registry.size

          val splitterKeys = splitterCalculator.calculate(allSamples, numWorkers)

          val splitters = Splitters(
            key = splitterKeys.map(k => com.google.protobuf.ByteString.copyFrom(k)).toSeq
          )

          responseObserver.onNext(splitters)
        } else {
          println(s"Waiting for more workers... (${sampleCollector.collectAllSamples().length} samples so far)")
          responseObserver.onNext(Splitters(key = Seq.empty))
        }

        responseObserver.onCompleted()
      }
    }
  }
}