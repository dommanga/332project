package worker

import io.grpc.{Server, ServerBuilder}
import rpc.sort.{WorkerServiceGrpc, PartitionPlan, Ack, PartitionChunk, TaskId}
import io.grpc.stub.StreamObserver
import scala.concurrent.{ExecutionContext, Future}

import scala.collection.mutable
import common.RecordIO
import java.nio.file.{Files, Paths, Path}
import java.io.FileOutputStream
import java.nio.ByteBuffer

object WorkerServer {
  def main(args: Array[String]): Unit = {
    if (args.length < 1) {
      println("Usage: worker-server <port> [outputDir]")
      System.exit(1)
    }

    val port      = args(0).toInt
    val outputDir = if (args.length >= 2) args(1) else "./out"

    val server = new WorkerServer(port, outputDir)
    server.start()
    server.blockUntilShutdown()
  }
}

class WorkerServer(port: Int, outputDir: String) {
  private var server: Server = _
  private implicit val ec: ExecutionContext = ExecutionContext.global
  private val impl = new WorkerServiceImpl(outputDir)

  def start(): Unit = {
    server = ServerBuilder
      .forPort(port)
      .addService(WorkerServiceGrpc.bindService(impl, ec))
      .build()
      .start()
    println(s"WorkerService listening on port $port (outputDir=$outputDir)")
  }

  def blockUntilShutdown(): Unit = {
    if (server != null) server.awaitTermination()
  }
}

class WorkerServiceImpl(outputDir: String)(implicit ec: ExecutionContext)
  extends WorkerServiceGrpc.WorkerService {

  // -----------------------------
  //  PartitionPlan 저장 (그대로 유지)
  // -----------------------------
  object PlanStore {
    @volatile private var latest: Option[PartitionPlan] = None
    def set(p: PartitionPlan): Unit = latest = Some(p)
    def get: Option[PartitionPlan] = latest
  }

  // -----------------------------
  //  Partition 데이터 저장소
  //   - partition_id 별로 "sorted run" 여러 개를 쌓아둠
  //   - run 하나 = Array[Array[Byte]] (각 원소가 100바이트 레코드)
  // -----------------------------
  object PartitionStore {
    // partitionId -> runs (각 run은 정렬된 record 배열)
    private val runsByPid =
      mutable.Map.empty[String, mutable.ArrayBuffer[Array[Array[Byte]]]]

    def addRun(partitionId: String, run: Array[Array[Byte]]): Unit = this.synchronized {
      val buf = runsByPid.getOrElseUpdate(partitionId, mutable.ArrayBuffer.empty)
      buf += run
      println(s"[Worker] Stored run for partition=$partitionId (records=${run.length}, totalRuns=${buf.size})")
    }

    /** 해당 partition의 run들을 꺼내면서 map에서 제거 */
    def drainRuns(partitionId: String): List[Array[Array[Byte]]] = this.synchronized {
      runsByPid.remove(partitionId).map(_.toList).getOrElse(Nil)
    }

    /** 현재까지 들어온 partition_id 전체 리스트 */
    def allPartitionIds: List[String] = this.synchronized {
      runsByPid.keys.toList
    }
  }

  // -----------------------------
  //  PartitionPlan 설정
  // -----------------------------
  override def setPartitionPlan(plan: PartitionPlan): Future[Ack] = {
    println(s"[Worker] Received PartitionPlan for task=${plan.task.map(_.id).getOrElse("unknown")}")
    plan.ranges.zipWithIndex.foreach { case (r, idx) =>
      println(f"  range#$idx → worker=${r.targetWorker}%d, " +
        s"lo=${bytesToHex(r.lo.toByteArray)} hi=${bytesToHex(r.hi.toByteArray)}")
    }
    PlanStore.set(plan)
    Future.successful(Ack(ok = true, msg = "Plan received"))
  }

  // -----------------------------
  //  Shuffle 수신: PushPartition
  //   - 한 stream = (task, partition_id)에 대한 "한 sender의 정렬된 run"
  //   - payload 안에는 여러 개의 100B record가 들어있음
  // -----------------------------
  override def pushPartition(responseObserver: StreamObserver[Ack]): StreamObserver[PartitionChunk] = {
    new StreamObserver[PartitionChunk] {
      private var countChunks: Long = 0L
      private var lastPid: String   = ""
      private var lastSeq: Long     = -1L
      private var currentPid: Option[String] = None

      // 이 stream 에서 받은 record들을 전부 모으는 버퍼
      private val recordBuffer = mutable.ArrayBuffer.empty[Array[Byte]]

      override def onNext(ch: PartitionChunk): Unit = {
        countChunks += 1
        lastPid = ch.partitionId
        lastSeq = ch.seq

        if (currentPid.isEmpty) {
          currentPid = Some(ch.partitionId)
        } else if (currentPid.get != ch.partitionId) {
          System.err.println(
            s"[Worker] pushPartition: mixed partitionIds in one stream: " +
              s"${currentPid.get} vs ${ch.partitionId}"
          )
        }

        // payload 안에서 100바이트 레코드들을 잘라냄
        val bytes  = ch.payload.toByteArray
        val recLen = RecordIO.RecordSize

        if (bytes.length % recLen != 0) {
          System.err.println(
            s"[Worker] WARNING: payload length ${bytes.length} is not multiple of RecordSize=$recLen"
          )
        }

        var offset = 0
        while (offset + recLen <= bytes.length) {
          val rec = java.util.Arrays.copyOfRange(bytes, offset, offset + recLen)
          recordBuffer += rec
          offset += recLen
        }
      }

      override def onError(t: Throwable): Unit = {
        System.err.println(s"[Worker] pushPartition stream error: ${t.getMessage}")
      }

      override def onCompleted(): Unit = {
        val pid = currentPid.getOrElse {
          System.err.println("[Worker] pushPartition completed with no data")
          ""
        }

        val run: Array[Array[Byte]] = recordBuffer.toArray
        if (pid.nonEmpty && run.nonEmpty) {
          PartitionStore.addRun(pid, run)
        }

        println(
          s"[Worker] pushPartition completed: partition=$pid chunks=$countChunks lastSeq=$lastSeq records=${run.length}"
        )

        responseObserver.onNext(Ack(ok = true, msg = s"received $countChunks chunks ($run.length records) for $pid"))
        responseObserver.onCompleted()
      }
    }
  }

  // -----------------------------
  //  K-way merge & 파일 쓰기 helper
  // -----------------------------

  /** record에서 key(앞 10바이트)를 뽑는 함수 */
  private def keyOf(rec: Array[Byte]): Array[Byte] =
    java.util.Arrays.copyOfRange(rec, 0, RecordIO.KeySize)

  /** record 두 개를 key 기준으로 비교 */
  private def compareRecords(a: Array[Byte], b: Array[Byte]): Int =
    RecordIO.compareKeys(keyOf(a), keyOf(b))

  /**
   * 여러 개의 "정렬된 run" (각 run은 Array[Array[Byte]])을 K-way merge 해서
   * 전체 오름차순 record iterator를 반환.
   */
  private def mergeRuns(runs: List[Array[Array[Byte]]]): Iterator[Array[Byte]] = {
    case class RunIter(var current: Array[Byte], it: Iterator[Array[Byte]])

    // Scala PriorityQueue 는 max-heap 이라, 최소 key가 먼저 나오게 비교 반전
    implicit val runOrdering: Ordering[RunIter] =
      Ordering.fromLessThan[RunIter] { (x, y) =>
        compareRecords(x.current, y.current) > 0 // current가 "더 큼"을 true로 → min-heap 효과
      }

    val pq = mutable.PriorityQueue.empty[RunIter]

    // 각 run의 첫 요소를 PQ에 넣기
    runs.foreach { runArr =>
      val it = runArr.iterator
      if (it.hasNext) {
        pq.enqueue(RunIter(it.next(), it))
      }
    }

    new Iterator[Array[Byte]] {
      override def hasNext: Boolean = pq.nonEmpty
      override def next(): Array[Byte] = {
        val smallest = pq.dequeue()
        val result   = smallest.current
        if (smallest.it.hasNext) {
          smallest.current = smallest.it.next()
          pq.enqueue(smallest)
        }
        result
      }
    }
  }

  /** partitionId에 해당하는 run들을 K-way merge 해서 파일로 쓰기 */
  def finalizePartition(partitionId: String): Unit = {
    val runs: List[Array[Array[Byte]]] = PartitionStore.drainRuns(partitionId)

    if (runs.isEmpty) {
      println(s"[Worker] finalizePartition($partitionId): no data")
      return
    }

    val mergedIter: Iterator[Array[Byte]] = mergeRuns(runs)
    writePartitionToFile(partitionId, mergedIter)
  }

  /** 현재까지 들어온 모든 partition_id에 대해 finalize */
  def finalizeAll(): Unit = {
    val pids = PartitionStore.allPartitionIds
    println(s"[Worker] finalizeAll: partitions=${pids.mkString(", ")}")
    pids.foreach(finalizePartition)
  }

  /** 최종 merged record들을 outputDir 아래 파일로 저장 */
  private def writePartitionToFile(partitionId: String, records: Iterator[Array[Byte]]): Unit = {
    val outDirPath: Path = Paths.get(outputDir)
    if (!Files.exists(outDirPath)) {
      Files.createDirectories(outDirPath)
    }

    // partition_id가 "p0" 형태라면 "partition.0" 으로 저장
    val fileName =
      if (partitionId.startsWith("p") && partitionId.drop(1).forall(_.isDigit)) {
        s"partition.${partitionId.drop(1)}"
      } else {
        partitionId
      }

    val filePath = outDirPath.resolve(fileName)
    val fos      = new FileOutputStream(filePath.toFile)
    val ch       = fos.getChannel

    try {
      records.foreach { rec =>
        val buf = ByteBuffer.wrap(rec)
        ch.write(buf)
      }
      println(s"[Worker] Wrote ${filePath.toAbsolutePath}")
    } finally {
      ch.close()
      fos.close()
    }
  }

  // Byte array → hex 문자열 (기존 함수 유지)
  private def bytesToHex(arr: Array[Byte]): String =
    arr.map("%02X".format(_)).mkString

  override def startShuffle(taskId: TaskId): Future[Ack] = {
    Future {
      println(s"[Worker] Received StartShuffle command for task=${taskId.id}")

      // TODO: Sangwon의 shuffle 로직 호출
      // WorkerClient.startShufflePhase() ??

      Ack(ok = true, msg = "Shuffle started")
    }
  }

  override def finalizePartitions(taskId: TaskId): Future[Ack] = {
    Future {
      println(s"[Worker] Received FinalizePartitions command for task=${taskId.id}")

      finalizeAll()
      reportMergeCompleteToMaster()
      WorkerState.signalFinalizeComplete()

      Ack(ok = true, msg = "Finalize complete")
    }
  }

  private def reportMergeCompleteToMaster(): Unit = {
    WorkerState.reportMergeComplete()
    println("[Worker] TODO: Report merge complete to Master")
  }
}
