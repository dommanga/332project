package worker

import io.grpc.{Server, ServerBuilder}
import rpc.sort.{WorkerServiceGrpc, PartitionPlan, Ack, PartitionChunk}
import io.grpc.stub.StreamObserver
import scala.concurrent.{ExecutionContext, Future}

object WorkerServer {
  def main(args: Array[String]): Unit = {
    if (args.length < 1) {
      println("Usage: worker-server <port>")
      System.exit(1)
    }
    val port = args(0).toInt
    val server = new WorkerServer(port)
    server.start()
    server.blockUntilShutdown()
  }
}

class WorkerServer(port: Int) {
  private var server: Server = _
  private implicit val ec: ExecutionContext = ExecutionContext.global
  private val impl = new WorkerServiceImpl

  def start(): Unit = {
    server = ServerBuilder
      .forPort(port)
      .addService(WorkerServiceGrpc.bindService(impl, ec))
      .build()
      .start()
    println(s"WorkerService listening on port $port")
  }

  def blockUntilShutdown(): Unit = {
    if (server != null) server.awaitTermination()
  }
}

class WorkerServiceImpl(implicit ec: ExecutionContext)
  extends WorkerServiceGrpc.WorkerService {

  override def setPartitionPlan(plan: PartitionPlan): Future[Ack] = {
    println(s"[Worker] Received PartitionPlan for task=${plan.task.map(_.id).getOrElse("unknown")}")
    plan.ranges.zipWithIndex.foreach { case (r, idx) =>
      println(f"  range#$idx → worker=${r.targetWorker}%d, " +
        s"lo=${bytesToHex(r.lo.toByteArray)} hi=${bytesToHex(r.hi.toByteArray)}")
    }
    Future.successful(Ack(ok = true, msg = "Plan received"))
  }
  override def pushPartition(responseObserver: StreamObserver[Ack]): StreamObserver[PartitionChunk] = {
    new StreamObserver[PartitionChunk] {
      private var count: Long = 0L
      private var lastPid: String = ""
      private var lastSeq: Long = -1L

      override def onNext(ch: PartitionChunk): Unit = {
        // 여기서 payload를 파일/버퍼에 쓰도록 확장 가능
        count += 1
        lastPid = ch.partitionId
        lastSeq = ch.seq
      }
      override def onError(t: Throwable): Unit = {
        System.err.println(s"[Worker] pushPartition stream error: ${t.getMessage}")
      }

      override def onCompleted(): Unit = {
        println(s"[Worker] pushPartition completed: partition=$lastPid chunks=$count lastSeq=$lastSeq")
        responseObserver.onNext(Ack(ok = true, msg = s"received $count chunks for $lastPid"))
        responseObserver.onCompleted()
      }
    }
  }

  // Byte array → hex 문자열
  private def bytesToHex(arr: Array[Byte]): String =
    arr.map("%02X".format(_)).mkString
}
