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
          port       = 0,
          inputDirs  = conf.inputPaths,
          outputDir  = conf.outputDir
        )

        val assignment = masterClient.register(workerInfo)
        println(s"➡️  assigned workerId = ${assignment.workerId}, port = ${assignment.assignedPort}")

        WorkerState.setMasterClient(masterClient)
        WorkerState.setWorkerId(assignment.workerId)

        val workerServer = new WorkerServer(assignment.assignedPort, conf.outputDir)
        workerServer.start()
        println(s"🔌 WorkerServer started on port ${assignment.assignedPort}")

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
          val file = new java.io.File(path)
          val files = if (file.isDirectory) {
            file.listFiles().filter(_.isFile).toSeq
          } else {
            Seq(file)
          }
          
          files.flatMap { f =>
            val buf = scala.collection.mutable.ArrayBuffer.empty[Array[Byte]]
            RecordIO.streamRecords(f.getPath) { (key, value) =>
              val rec = new Array[Byte](RecordIO.RecordSize)
              System.arraycopy(key, 0, rec, 0, RecordIO.KeySize)
              System.arraycopy(value, 0, rec, RecordIO.KeySize, RecordIO.RecordSize - RecordIO.KeySize)
              buf += rec
            }
            buf
          }.toVector
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
        println("🔑 Local sorting completed")

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
        // 7) PartitionPlan에서 Worker 주소 대기 및 수신
        // ---------------------------------------------------------
        println("⏳ Waiting for PartitionPlan with worker addresses...")
        
        // WorkerServer의 PlanStore에서 Plan을 받을 때까지 대기
        var workerAddresses: Map[Int, (String, Int)] = Map.empty
        val planDeadline = System.nanoTime() + 60_000_000_000L // 60초 대기
        
        while (workerAddresses.isEmpty && System.nanoTime() < planDeadline) {
          Thread.sleep(100)
          // WorkerServer에서 저장한 Plan 확인
          WorkerState.getWorkerAddresses match {
            case Some(addrs) if addrs.nonEmpty =>
              workerAddresses = addrs
              println(s"📋 Received worker addresses: ${addrs.map { case (id, (ip, port)) => s"$id->$ip:$port" }.mkString(", ")}")
            case _ =>
              // 아직 Plan 미수신
          }
        }
        
        if (workerAddresses.isEmpty) {
          throw new RuntimeException("Timeout waiting for PartitionPlan with worker addresses")
        }

        // ---------------------------------------------------------
        // 8) Shuffle 송신 - 실제 Worker IP 사용
        // ---------------------------------------------------------
          def sendPartitionWithRetry(
            originalTarget: Int,
            partitionId: Int,
            records: Seq[Array[Byte]],
            workerAddresses: Map[Int, (String, Int)],
            maxRetries: Int = 3
          ): Unit = {
            
            var attempt = 0
            
            while (attempt < maxRetries) {
              // 현재 target 확인 (reassignment 반영)
              val currentTarget = WorkerState.getTarget(partitionId, originalTarget)
              
              try {
                val (targetIp, targetPort) = workerAddresses(currentTarget)
                println(s"  Attempt ${attempt+1}/$maxRetries: p$partitionId → worker#$currentTarget ($targetIp:$targetPort)")
                
                val channel = ManagedChannelBuilder
                  .forAddress(targetIp, targetPort)
                  .usePlaintext()
                  .build()
                
                val stub = WorkerServiceGrpc.stub(channel)
                val ackPromise = scala.concurrent.Promise[Unit]()
                
                val responseObserver = new StreamObserver[Ack] {
                  override def onNext(v: Ack): Unit =
                    println(s"    ✓ ACK from worker#$currentTarget: ${v.msg}")
                  
                  override def onError(t: Throwable): Unit = {
                    println(s"    ✗ Error: ${t.getMessage}")
                    ackPromise.failure(t)
                  }
                  
                  override def onCompleted(): Unit = {
                    println(s"    ✓ Completed p$partitionId")
                    ackPromise.success(())
                  }
                }
                
                val requestObserver = stub.pushPartition(responseObserver)
                
                var seq: Long = 0
                records.foreach { rec =>
                  val chunk = PartitionChunk(
                    task = Some(TaskId("task-001")),
                    partitionId = s"p$partitionId",
                    payload = ByteString.copyFrom(rec),
                    seq = seq
                  )
                  seq += 1
                  requestObserver.onNext(chunk)
                }
                
                requestObserver.onCompleted()
                Await.result(ackPromise.future, 30.seconds)
                channel.shutdown()
                
                println(s"  ✅ p$partitionId sent successfully")
                return  // 성공! 함수 종료
                
              } catch {
                case e: Exception =>
                  attempt += 1
                  
                  if (attempt < maxRetries) {
                    val backoff = 2000 * attempt  // 2s, 4s, 6s
                    println(s"  ⚠️ Send failed, retry after ${backoff}ms: ${e.getMessage}")
                    Thread.sleep(backoff)
                    
                    // Reassignment 확인
                    val newTarget = WorkerState.getTarget(partitionId, originalTarget)
                    if (newTarget != currentTarget) {
                      println(s"  ℹ️ Target changed: worker#$currentTarget → worker#$newTarget")
                      attempt = 0  // 새 target이면 attempt reset!
                    }
                  } else {
                    Console.err.println(s"  ❌ Failed to send p$partitionId after $maxRetries attempts")
                    throw new RuntimeException(s"Failed after $maxRetries attempts", e)
                  }
              }
            }
          }

        println("-------------------------------------------------------")
        println("     🚚 Starting Shuffle: worker → worker")
        println("-------------------------------------------------------")

        try {
          for ((pid, recs) <- partitioned) {
            val targetWorker = pid % workerAddresses.size
            sendPartitionWithRetry(targetWorker, pid, recs, workerAddresses)
          }
        } catch {
          case e: Exception =>
            Console.err.println(s"❌ Shuffle failed: ${e.getMessage}")
            Console.err.println("Note: Sender failure recovery not yet implemented")
            throw e
        }

        println("-------------------------------------------------------")
        println("       🎉 Shuffle Completed")
        println("-------------------------------------------------------")
        
        // Shuffle 완료 보고
        WorkerState.reportShuffleComplete()

        println("⏳ Waiting for finalize command from Master...")
        WorkerState.awaitFinalizeComplete()
        println("✅ Worker completed successfully")

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

    val masterAddr = args(0).split(":", 2)
    if (masterAddr.length != 2) {
      Console.err.println("Invalid master address format. Use HOST:PORT")
      return None
    }
  
    val masterHost = masterAddr(0)
    val masterPort = masterAddr(1).toInt
    val inputs     = collection.mutable.ArrayBuffer.empty[String]
    var outputDir  = "./out"
    var workerId   = "worker-1"

    var i = 1
    def needValue(opt: String): Boolean = {
      if (i >= args.length) {
        Console.err.println(s"Missing value for $opt")
        false
      } else true
    }

    while (i < args.length) {
      args(i) match {
        case "-I" | "--input" =>
          i += 1
          if (!needValue("-I")) return None
          inputs += args(i)

        case "-O" | "--output" =>
          i += 1
          if (!needValue("-O")) return None
          outputDir = args(i)

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
          workerId   = workerId
        )
      )
    }
  }

  private def printUsage(): Unit = {
    val msg =
      """Usage:
        |  worker <master IP:port> -I <input directory> [<input directory> ...] -O <output directory>
        |
        |Example:
        |  worker 141.223.91.80:30040 -I /data1/input /data2/input -O /home/gla/data
        |""".stripMargin
    Console.err.println(msg)
  }
}