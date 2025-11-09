package worker

final case class WorkerConfig(
  masterHost: String,
  masterPort: Int,
  inputPaths: Seq[String],
  outputDir: String,
  workerId: String,
  workerPort: Int
)

object WorkerClient extends App {

  parseArgs(args) match {
    case Some(conf) =>
      println("✅ Worker started with config:")
      println(s"  master   = ${conf.masterHost}:${conf.masterPort}")
      println(s"  inputs   = ${conf.inputPaths.mkString(", ")}")
      println(s"  output   = ${conf.outputDir}")
      println(s"  id       = ${conf.workerId}")
      println(s"  port     = ${conf.workerPort}")

      // --- Week4: 샘플링 테스트 ---
      val samples = Sampling.sampleKeysEveryN(conf.inputPaths, step = 1000)
      println(s"  collected ${samples.size} sample keys (every 1000th record)")
      // TODO(다음 단계):
      //   - samples 를 Master에 gRPC client-streaming으로 전송

    case None =>
      sys.exit(1)
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
