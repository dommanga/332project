package worker

import rpc.sort._
import scala.concurrent.{Await, ExecutionContext}
import scala.concurrent.duration._
import io.grpc.ManagedChannelBuilder
import io.grpc.stub.StreamObserver
import com.google.protobuf.ByteString
import common.RecordIO

/** Worker 실행 초기 설정 */
final case class WorkerConfig(
    masterHost: String,
    masterPort: Int,
    inputPaths: Seq[String],
    outputDir: String,
    workerId: String,
    workerPort: Int
)

/** Worker 실행 메인 */
object WorkerClient extends App {

  implicit val ec: ExecutionContext = ExecutionContext.global

  parseArgs(args) match {
    case Some(conf) =>
      println("=============================================")
      println("   ✅ Worker started with config:")
      println(s"      master   = ${conf.masterHost}:${conf.masterPort}")
      println(s"      inputs   = ${conf.inputPaths.mkString(", ")}")
      println(s"      output   = ${conf.outputDir}")
      println(s"      id       = ${conf.workerId}")
      println(s"      port     = ${conf.workerPort}")
      println("=============================================")

      // Master 클라이언트 생성
      val masterClient = new MasterClient(conf.masterHost, conf.masterPort)

      try {
        // ---------------------------------------------------------
        // 1) Worker 등록
        // ---------------------------------------------------------
        val workerInfo = WorkerInfo(
          id         = conf.workerId,
          ip         = getLocalIP(),
          port       = conf.workerPort,
          inputDirs  = conf.inputPaths,
          outputDir  = conf.outputDir
        )

        val assignment = masterClient.register(workerInfo)
        println(s"➡️  assigned workerId = ${assignment.workerId}")
        println(s"➡️  assigned partitions = ${assignment.partitionIds.mkString("[", ", ", "]")}")

        WorkerState.setMasterClient(masterClient)
        WorkerState.setWorkerId(assignment.workerId)

        val workerServer = new WorkerServer(conf.workerPort, conf.outputDir)
        workerServer.start()

        // ---------------------------------------------------------
        // 2) 샘플링
        // ---------------------------------------------------------
        val samples = common.Sampling.uniformEveryN(conf.inputPaths, everyN = 1000)
        println(s"➡️  collected ${samples.size} sample keys")

        // ---------------------------------------------------------
        // 3) Splitters 수신
        // ---------------------------------------------------------
        val splitters = masterClient.sendSamples(samples)
        println(s"➡️  received ${splitters.key.size} splitters from Master")

        // ---------------------------------------------------------
        // TODO Week 5: 실제 정렬 + 파티셔닝 + Shuffle 송신
        // ---------------------------------------------------------

        println("-------------------------------------------------------")
        println("    🚀 [Week5] Local sorting + partitioning + shuffle")
        println("-------------------------------------------------------")

        // ---------------------------------------------------------
        // Helper 1: extract key from 100-byte record
        // ---------------------------------------------------------
        def extractKey(rec: Array[Byte]): Array[Byte] =
          java.util.Arrays.copyOfRange(rec, 0, RecordIO.KeySize)

        // ---------------------------------------------------------
        // Helper 2: compare two keys (as Boolean)
        // ---------------------------------------------------------
        def lessThan(a: Array[Byte], b: Array[Byte]): Boolean =
          RecordIO.compareKeys(a, b) < 0

        // ---------------------------------------------------------
        // Helper 3: read all 100-byte records from files
        // ---------------------------------------------------------
        def readAll(path: String): Vector[Array[Byte]] = {
          val buf = scala.collection.mutable.ArrayBuffer.empty[Array[Byte]]
          RecordIO.streamRecords(path) { (key, value) =>
            val rec = new Array[Byte](RecordIO.RecordSize)
            System.arraycopy(key, 0, rec, 0, RecordIO.KeySize)
            System.arraycopy(value, 0, rec, RecordIO.KeySize, RecordIO.RecordSize - RecordIO.KeySize)
            buf += rec
          }
          buf.toVector
        }

        // ---------------------------------------------------------
        // 4) 모든 input 레코드 읽기
        // ---------------------------------------------------------
        val allRecords: Vector[Array[Byte]] =
          conf.inputPaths.flatMap(path => readAll(path)).toVector

        println(s"📦 Loaded total ${allRecords.size} records")

        // ---------------------------------------------------------
        // 5) Local Sort (key 기반)
        // ---------------------------------------------------------
        val sorted = allRecords.sortWith { (a, b) =>
  		RecordIO.compareKeys(extractKey(a), extractKey(b)) < 0
	}
        println("📑 Local sorting completed")

        // ---------------------------------------------------------
        // 6) Splitters 기반 Partitioning
        // ---------------------------------------------------------
        val splitterKeys: Array[Array[Byte]] =
          splitters.key.map(_.toByteArray).toArray

        def findPartition(key: Array[Byte]): Int = {
          var idx = 0
          while (idx < splitterKeys.length &&
                 lessThan(splitterKeys(idx), key)) {
            idx += 1
          }
          idx
        }

        val partitioned =
          sorted.groupBy(rec => findPartition(extractKey(rec)))

        println(s"🧩 Partitioning complete → partitions=${partitioned.size}")

        // ---------------------------------------------------------
        // 7) Shuffle 송신
        // ---------------------------------------------------------

        /** Worker 포트 규칙:
          *   worker0 → 6000
          *   worker1 → 6001
          *   worker2 → 6002
          */
        def targetPort(workerId: Int): Int = 6000 + workerId

        def sendPartition(
            targetWorkerId: Int,
            partitionId: Int,
            records: Seq[Array[Byte]]
        ): Unit = {

          val port = targetPort(targetWorkerId)
          println(s"➡️  Sending partition p$partitionId → worker#$targetWorkerId (port=$port)")

          val channel =
            ManagedChannelBuilder.forAddress("localhost", port)
              .usePlaintext()
              .build()

          val stub = WorkerServiceGrpc.stub(channel)

          val ackPromise = scala.concurrent.Promise[Unit]()

          val responseObserver = new StreamObserver[Ack] {
            override def onNext(v: Ack): Unit =
              println(s"   ✔ ACK from worker#$targetWorkerId: ${v.msg}")

            override def onError(t: Throwable): Unit = {
              println(s"   ❌ Error sending partition to worker#$targetWorkerId : ${t.getMessage}")
              ackPromise.failure(t)
            }

            override def onCompleted(): Unit = {
              println(s"   ✔ Completed sending partition p$partitionId")
              ackPromise.success(())
            }
          }

          val requestObserver =
            stub.pushPartition(responseObserver)

          var seq: Long = 0
          records.foreach { rec =>
            val chunk = PartitionChunk(
              task        = Some(TaskId("task-001")),
              partitionId = s"p$partitionId",
              payload     = ByteString.copyFrom(rec),
              seq         = seq
            )
            seq += 1
            requestObserver.onNext(chunk)
          }

          requestObserver.onCompleted()
          Await.result(ackPromise.future, Duration.Inf)
          channel.shutdown()
        }

        println("-------------------------------------------------------")
        println("     🚚 Starting Shuffle: worker → worker")
        println("-------------------------------------------------------")

        for ((pid, recs) <- partitioned) {
          val targetWorker = pid % assignment.partitionIds.size
          sendPartition(targetWorker, pid, recs)
        }

        println("-------------------------------------------------------")
        println("       🎉 Shuffle Completed")
        println("-------------------------------------------------------")

      } finally {
        masterClient.shutdown()
      }

    case None =>
      sys.exit(1)
  }

  /** Local IPv4 검색 */
  private def getLocalIP(): String = {
    import java.net.{InetAddress, NetworkInterface}
    import scala.jdk.CollectionConverters._

    NetworkInterface.getNetworkInterfaces.asScala
      .flatMap(_.getInetAddresses.asScala)
      .find(addr => !addr.isLoopbackAddress && addr.getAddress.length == 4)
      .map(_.getHostAddress)
      .getOrElse("127.0.0.1")
  }

  // ---------------------------------------------------------
  // CLI 입력 파서
  // ---------------------------------------------------------
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

