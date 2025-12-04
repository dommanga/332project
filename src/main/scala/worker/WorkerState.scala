package worker

import scala.collection.mutable
import java.util.concurrent.CountDownLatch
import rpc.sort._

object WorkerState {
  @volatile private var _masterClient: Option[MasterClient] = None
  @volatile private var _workerInfo: Option[WorkerInfo] = None
  @volatile private var _workerAddresses: Option[Map[Int, (String, Int)]] = None
  
  private var _shuffleReport: Option[ShuffleCompletionReport] = None
  private val finalizeLatch = new CountDownLatch(1)

  // ===== WorkerInfo 관련 =====
  def setWorkerInfo(info: WorkerInfo): Unit = {
    _workerInfo = Some(info)
    println(s"[WorkerState] Stored WorkerInfo: id=${info.id}, port=${info.port}")
  }

  def getWorkerInfo: Option[WorkerInfo] = _workerInfo

  // Convenience getters
  def getWorkerId: Int = _workerInfo.map(_.id.toInt).getOrElse(-1)
  
  def getInputPaths: Seq[String] = _workerInfo.map(_.inputDirs).getOrElse(Seq.empty)

  // ===== MasterClient 관련 =====
  def setMasterClient(client: MasterClient): Unit = {
    _masterClient = Some(client)
  }

  def getMasterClient: MasterClient = {
    _masterClient.getOrElse {
      throw new RuntimeException("MasterClient not set!")
    }
  }

  // ===== Worker Addresses 관련 =====
  def setWorkerAddresses(addresses: Map[Int, (String, Int)]): Unit = {
    _workerAddresses = Some(addresses)
    println(s"[WorkerState] Stored ${addresses.size} worker addresses")
  }

  def getWorkerAddresses: Option[Map[Int, (String, Int)]] = _workerAddresses

  // ===== Shuffle Report 관련 =====
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

  // ===== Merge Complete 관련 =====
  def reportMergeComplete(): Unit = {
    _masterClient match {
      case Some(client) =>
        client.reportMergeComplete(getWorkerId)
      case None =>
        System.err.println("[WorkerState] MasterClient not set!")
    }
  }

  // ===== Finalize 관련 =====
  def signalFinalizeComplete(): Unit = {
    finalizeLatch.countDown()
  }

  def awaitFinalizeComplete(): Unit = {
    finalizeLatch.await()
  }
}