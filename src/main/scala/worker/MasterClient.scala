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

  private val heartbeatChannel: ManagedChannel = ManagedChannelBuilder
    .forAddress(host, port)
    .usePlaintext()
    .build()

  private val stub = MasterServiceGrpc.stub(channel)
  private val blockingStub = MasterServiceGrpc.blockingStub(channel)

  private val heartbeatStub = MasterServiceGrpc.blockingStub(heartbeatChannel)

  /** Worker 등록 */
  def register(workerInfo: WorkerInfo): WorkerAssignment = {
    blockingStub.registerWorker(workerInfo)
  }

  /** Heartbeat 전송 */
  def sendHeartbeat(workerInfo: WorkerInfo): Unit = {
    heartbeatStub.heartbeat(workerInfo)
  }

  /** 샘플 전송 (Client Streaming) */
  def sendSamples(samples: Seq[Array[Byte]]): Splitters = {
    println(s"📊 Sending ${samples.size} samples to Master...")

    val promise = Promise[Splitters]()

    val responseObserver = new StreamObserver[Splitters] {
      override def onNext(splitters: Splitters): Unit = {
        promise.success(splitters)
      }
      override def onError(t: Throwable): Unit = {
        Console.err.println(s"❌ Error receiving splitters: ${t.getMessage}")
        promise.failure(t)
      }
      override def onCompleted(): Unit = {}
    }

    val requestObserver = stub.sendSamples(responseObserver)

    try {
      samples.foreach { keyBytes =>
        val sample = Sample(
          key = com.google.protobuf.ByteString.copyFrom(keyBytes)
        )
        requestObserver.onNext(sample)
      }

      requestObserver.onCompleted()

      import scala.concurrent.Await
      Await.result(promise.future, 120.seconds)

    } catch {
      case e: Exception =>
        requestObserver.onError(e)
        throw e
    }
  }

  def reportShuffleComplete(report: ShuffleCompletionReport): Unit = {
    try {
      blockingStub
        .withDeadlineAfter(15, java.util.concurrent.TimeUnit.SECONDS)
        .reportShuffleComplete(report)
    } catch {
      case e: io.grpc.StatusRuntimeException =>
        Console.err.println(s"⚠️ Non-fatal: Shuffle report failed: ${e.getStatus}")
        throw e
    }
  }

  def reportMergeComplete(workerId: Int): Unit = {
    try {
      val status = WorkerStatus(workerId = workerId)
      blockingStub
        .withDeadlineAfter(15, java.util.concurrent.TimeUnit.SECONDS)
        .reportMergeComplete(status)
    } catch {
      case e: io.grpc.StatusRuntimeException =>
        Console.err.println(s"⚠️ Non-fatal: Merge report failed: ${e.getStatus}")
        throw e
    }
  }

  def queryPartitionSenders(partitionId: Int): Seq[Int] = {
    val query = PartitionSendersQuery(partitionId = partitionId)
    val response = blockingStub.queryPartitionSenders(query)
    response.senderIds
  }

  /** 연결 종료 */
  def shutdown(): Unit = {
    channel.shutdown()
    heartbeatChannel.shutdown()
    try {
      channel.awaitTermination(5, java.util.concurrent.TimeUnit.SECONDS)
      heartbeatChannel.awaitTermination(5, java.util.concurrent.TimeUnit.SECONDS)
    } catch {
      case _: InterruptedException =>
        channel.shutdownNow()
        heartbeatChannel.shutdownNow()
        println(s"🛑 Shutdown command received")
    }
  }
}