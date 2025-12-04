package worker

import scala.collection.mutable
import java.util.concurrent.CountDownLatch
import rpc.sort._

object WorkerState {
  @volatile private var _masterClient: Option[MasterClient] = None
  @volatile private var _workerId: Int = -1
  @volatile private var _workerAddresses: Option[Map[Int, (String, Int)]] = None

  private var _inputPaths: Seq[String] = Seq.empty

  private var _shuffleReport: Option[ShuffleCompletionReport] = None

  private val finalizeLatch = new CountDownLatch(1)

  def setInputPaths(paths: Seq[String]): Unit = {
    _inputPaths = paths
    println(s"[WorkerState] Stored ${paths.size} input paths")
  }

  def getInputPaths: Seq[String] = _inputPaths

  def setMasterClient(client: MasterClient): Unit = {
    _masterClient = Some(client)
  }

  def getMasterClient: MasterClient = {
  _masterClient.getOrElse {
      throw new RuntimeException("MasterClient not set!")
    }
  }

  def setWorkerId(id: Int): Unit = {
    _workerId = id
  }

  def getWorkerId: Int = _workerId

  /**
   * PartitionPlan에서 받은 Worker 주소들을 저장
   * @param addresses Map[workerId -> (ip, port)]
   */
  def setWorkerAddresses(addresses: Map[Int, (String, Int)]): Unit = {
    _workerAddresses = Some(addresses)
    println(s"[WorkerState] Stored ${addresses.size} worker addresses")
  }

  /**
   * 저장된 Worker 주소 반환
   */
  def getWorkerAddresses: Option[Map[Int, (String, Int)]] = _workerAddresses

    def setShuffleReport(report: ShuffleCompletionReport): Unit = {
    _shuffleReport = Some(report)
    println(s"[WorkerState] Shuffle report stored: ${report.sendRecords.size} records")
  }

  def reportShuffleComplete(): Unit = {
    (_masterClient, _shuffleReport) match {
      case (Some(client), Some(report)) =>
        client.reportShuffleComplete(report)
        println(s"[WorkerState] Shuffle report sent to Master")
      case (None, _) =>
        System.err.println("[WorkerState] MasterClient not set!")
      case (_, None) =>
        System.err.println("[WorkerState] Shuffle report not set!")
    }
  }

  /**
   * Merge 완료를 Master에 보고
   */
  def reportMergeComplete(): Unit = {
    _masterClient match {
      case Some(client) =>
        client.reportMergeComplete(_workerId)
      case None =>
        System.err.println("[WorkerState] MasterClient not set!")
    }
  }

  def signalFinalizeComplete(): Unit = {
    finalizeLatch.countDown()
  }

  def awaitFinalizeComplete(): Unit = {
    finalizeLatch.await()
  }
}