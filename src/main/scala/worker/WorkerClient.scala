package worker

import io.grpc.{ManagedChannel, ManagedChannelBuilder}
import rpc.sort.{MasterServiceGrpc, WorkerInfo}
import scala.concurrent.{Await, ExecutionContext}
import scala.concurrent.duration._
import scala.util.{Try, Success, Failure}

object WorkerClient {
  implicit val ec: ExecutionContext = ExecutionContext.global

  def main(args: Array[String]): Unit = {
    // Parse command line arguments
    if (args.length < 2) {
      println("Usage: worker <master_ip:port> -I <input_dir1> <input_dir2> ... -O <output_dir>")
      System.exit(1)
    }

    val masterAddr = args(0)  // "192.168.1.1:5000"
    val (inputDirs, outputDir) = parseArgs(args.drop(1))

    println(s"Worker starting...")
    println(s"Master address: $masterAddr")
    println(s"Input directories: ${inputDirs.mkString(", ")}")
    println(s"Output directory: $outputDir")

    // Connect to Master
    val channel = createChannel(masterAddr)
    val stub = MasterServiceGrpc.blockingStub(channel)

    try {
      // Register with Master
      val localIP = getLocalIP()
      val request = WorkerInfo(
        id = "",
        ip = localIP,
        port = 0,  // Week 3: Worker doesn't serve yet
        inputDirs = inputDirs,
        outputDir = outputDir
      )

      println(s"Registering with Master at $masterAddr...")
      val response = stub.registerWorker(request)

      println(s"✓ Registration successful!")
      println(s"  Worker ID: ${response.workerId}")
      println(s"  Assigned partitions: ${response.partitionIds.mkString(", ")}")

      // Wait for shutdown
      println("\nWorker registered. Press Enter to exit...")
      scala.io.StdIn.readLine()

    } catch {
      case e: Exception =>
        println(s"✗ Registration failed: ${e.getMessage}")
        e.printStackTrace()
    } finally {
      channel.shutdown()
      channel.awaitTermination(5, SECONDS)
      println("Worker shut down.")
    }
  }

  // Parse command line arguments
  private def parseArgs(args: Array[String]): (Seq[String], String) = {
    val iIndex = args.indexOf("-I")
    val oIndex = args.indexOf("-O")

    if (iIndex == -1 || oIndex == -1) {
      throw new IllegalArgumentException("Missing -I or -O flag")
    }

    val inputDirs = args.slice(iIndex + 1, oIndex).toSeq
    val outputDir = args(oIndex + 1)

    if (inputDirs.isEmpty) {
      throw new IllegalArgumentException("No input directories specified")
    }

    (inputDirs, outputDir)
  }

  // Create gRPC channel
  private def createChannel(target: String): ManagedChannel = {
    ManagedChannelBuilder
      .forTarget(target)
      .usePlaintext()
      .build()
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