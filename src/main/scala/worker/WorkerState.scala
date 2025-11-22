package worker

import java.util.concurrent.CountDownLatch

object WorkerState {
  @volatile private var _masterClient: Option[MasterClient] = None
  @volatile private var _workerId: Int = -1
  @volatile private var _workerAddresses: Option[Map[Int, (String, Int)]] = None

  private val finalizeLatch = new CountDownLatch(1)

  def setMasterClient(client: MasterClient): Unit = {
    _masterClient = Some(client)
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

  /**
   * Shuffle 완료를 Master에 보고
   */
  def reportShuffleComplete(): Unit = {
    _masterClient match {
      case Some(client) =>
        client.reportShuffleComplete(_workerId)
      case None =>
        System.err.println("[WorkerState] MasterClient not set!")
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