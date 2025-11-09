package worker

import rpc.sort.WorkerInfo
import scala.concurrent.ExecutionContext

final case class WorkerConfig(
                               masterHost: String,
                               masterPort: Int,
                               inputPaths: Seq[String],
                               outputDir: String,
                               workerId: String,
                               workerPort: Int
                             )

object WorkerClient extends App {

  implicit val ec: ExecutionContext = ExecutionContext.global

  parseArgs(args) match {
    case Some(conf) =>
      println("✅ Worker started with config:")
      println(s"  master   = ${conf.masterHost}:${conf.masterPort}")
      println(s"  inputs   = ${conf.inputPaths.mkString(", ")}")
      println(s"  output   = ${conf.outputDir}")
      println(s"  id       = ${conf.workerId}")
      println(s"  port     = ${conf.workerPort}")

      // Master 클라이언트 생성
      val masterClient = new MasterClient(conf.masterHost, conf.masterPort)

      try {
        // 1. Master에 등록
        val workerInfo = WorkerInfo(
          id = conf.workerId,
          ip = getLocalIP(),
          port = conf.workerPort,
          inputDirs = conf.inputPaths,
          outputDir = conf.outputDir
        )

        val assignment = masterClient.register(workerInfo)
        println(s"   assigned partitions: ${assignment.partitionIds.mkString("[", ", ", "]")}")

        // 2. 샘플링
        val samples = common.Sampling.uniformEveryN(conf.inputPaths, everyN = 1000)
        println(s"  collected ${samples.size} sample keys (every 1000th record)")

        // 3. 샘플 전송
        val splitters = masterClient.sendSamples(samples)
        println(s"  received ${splitters.key.size} splitters from Master")

        // TODO Week 5: 실제 정렬 & 파티셔닝

      } finally {
        masterClient.shutdown()
      }

    case None =>
      sys.exit(1)
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

  // ---------------------------
  // 아래는 단순 CLI 파서
  // ---------------------------
  private def parseArgs(args: Array[String]): Option[WorkerConfig] = {
    if (args.isEmpty) {
      printUsage()
      return None
    }

    var masterHost = "localhost"
    var masterPort = 5000
    val inputs     = collection.mutable.ArrayBuffer.empty[String]
    var outputDir  = "./out"
    var workerId   = "worker-1"
    var workerPort = 6000

    var i = 0
    def needValue(opt: String): Boolean = {
      if (i >= args.length) {
        Console.err.println(s"Missing value for $opt")
        false
      } else true
    }

    while (i < args.length) {
      args(i) match {
        case "--master" =>
          i += 1
          if (!needValue("--master")) return None
          val hp = args(i).split(":", 2)
          if (hp.length != 2 || !hp(1).forall(_.isDigit)) {
            Console.err.println("Invalid --master HOST:PORT")
            return None
          }
          masterHost = hp(0)
          masterPort = hp(1).toInt

        case "-I" | "--input" =>
          i += 1
          if (!needValue("-I")) return None
          inputs += args(i)

        case "-O" | "--output" =>
          i += 1
          if (!needValue("-O")) return None
          outputDir = args(i)

        case "--id" =>
          i += 1
          if (!needValue("--id")) return None
          workerId = args(i)

        case "--port" =>
          i += 1
          if (!needValue("--port")) return None
          workerPort = args(i).toInt

        case other =>
          Console.err.println(s"Unknown option: $other")
          printUsage()
          return None
      }
      i += 1
    }

    if (inputs.isEmpty) {
      Console.err.println("At least one -I <input-path> is required.")
      printUsage()
      None
    } else {
      Some(
        WorkerConfig(
          masterHost = masterHost,
          masterPort = masterPort,
          inputPaths = inputs.toSeq,
          outputDir  = outputDir,
          workerId   = workerId,
          workerPort = workerPort
        )
      )
    }
  }

  private def printUsage(): Unit = {
    val msg =
      """Usage:
        |  sbt "runMain worker.WorkerClient --master HOST:PORT \
        |                               -I INPUT_PATH [-I INPUT_PATH ...] \
        |                               -O OUTPUT_DIR \
        |                               --id WORKER_ID \
        |                               --port PORT"
        |
        |Example:
        |  sbt "runMain worker.WorkerClient --master 127.0.0.1:5000 \
        |                               -I data/part0 -I data/part1 \
        |                               -O out \
        |                               --id worker0 \
        |                               --port 6000"
        |""".stripMargin
    Console.err.println(msg)
  }
}
