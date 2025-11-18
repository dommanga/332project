package worker

import java.util.concurrent.CountDownLatch

object WorkerState {
  @volatile private var _masterClient: Option[MasterClient] = None
  @volatile private var _workerId: Int = -1

  private val finalizeLatch = new CountDownLatch(1)

  def setMasterClient(client: MasterClient): Unit = {
    _masterClient = Some(client)
  }

  def setWorkerId(id: Int): Unit = {
    _workerId = id
  }

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