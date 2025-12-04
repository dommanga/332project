package worker

import io.grpc.{ManagedChannel, ManagedChannelBuilder}
import io.grpc.stub.StreamObserver
import rpc.sort._
import scala.concurrent.{Promise, ExecutionContext}
import scala.concurrent.duration._
import java.util.concurrent.TimeUnit

class MasterClient(host: String, port: Int)(implicit ec: ExecutionContext) {

  private val channel: ManagedChannel =
    ManagedChannelBuilder
      .forAddress(host, port)
      .usePlaintext()
      .build()

  private val blockingStub = MasterServiceGrpc.blockingStub(channel)
  private val asyncStub = MasterServiceGrpc.stub(channel)

  /** Worker 등록 */
  def register(workerInfo: WorkerInfo): WorkerAssignment = {
    println(s"🔌 Connecting to Master at $host:$port...")
    val response = blockingStub.registerWorker(workerInfo)
    println(s"Registered as Worker #${response.workerId}")
    response
  }

  /** Heartbeat 전송 */
  def sendHeartbeat(workerInfo: WorkerInfo): Unit = {
    val ack = blockingStub.heartbeat(workerInfo)
    if (ack.ok) {
      println(s"Heartbeat sent")
    }
  }

  /** 샘플 전송 (Client Streaming) */
  def sendSamples(samples: Seq[Array[Byte]]): Splitters = {
    println(s"Sending ${samples.size} samples to Master...")

    val promise = Promise[Splitters]()

    // 응답을 받을 Observer
    val responseObserver = new StreamObserver[Splitters] {
      override def onNext(splitters: Splitters): Unit = {
        promise.success(splitters)
      }
      override def onError(t: Throwable): Unit = {
        Console.err.println(s"Error receiving splitters: ${t.getMessage}")
        promise.failure(t)
      }
      override def onCompleted(): Unit = {
        println("Splitters received from Master")
      }
    }

    // 샘플을 보낼 Observer
    val requestObserver = asyncStub.sendSamples(responseObserver)

    try {
      // 샘플 스트리밍 전송
      samples.foreach { keyBytes =>
        val sample = Sample(
          key = com.google.protobuf.ByteString.copyFrom(keyBytes)
        )
        requestObserver.onNext(sample)
      }

      // 전송 완료
      requestObserver.onCompleted()

      // 응답 대기 (최대 30초)
      import scala.concurrent.Await
      Await.result(promise.future, 120.seconds)

    } catch {
      case e: Exception =>
        requestObserver.onError(e)
        throw e
    }
  }

  def reportShuffleComplete(report: ShuffleCompletionReport): Unit = {
    val ack = blockingStub.reportShuffleComplete(report)
    println(s"[MasterClient] Shuffle report sent: ${ack.msg}")
  }

  def reportMergeComplete(workerId: Int): Unit = {
    val status = WorkerStatus(
      workerId = workerId,
      phase = "merge_complete",
      timestamp = System.currentTimeMillis()
    )
    val ack = blockingStub.reportMergeComplete(status)
    println(s"[MasterClient] Merge complete reported: ${ack.msg}")
  }

  def queryPartitionSenders(partitionId: Int): Seq[Int] = {
    val query = PartitionSendersQuery(partitionId = partitionId)
    val response = blockingStub.queryPartitionSenders(query)
    response.senderIds
  }

  /** 연결 종료 */
  def shutdown(): Unit = {
    channel.shutdown()
    channel.awaitTermination(5, TimeUnit.SECONDS)
  }
}