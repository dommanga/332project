package worker

import scala.collection.mutable
import java.util.concurrent.CountDownLatch
import rpc.sort._
import common.RecordIO

object WorkerState {
  @volatile private var _masterClient: Option[MasterClient] = None
  @volatile private var _workerInfo: Option[WorkerInfo] = None
  @volatile private var _workerAddresses: Option[Map[Int, (String, Int)]] = None
  @volatile private var _partitionPlan: Option[PartitionPlan] = None
  @volatile private var shutdownSignaled = false
  
  private var _shuffleReport: Option[ShuffleCompletionReport] = None
  private val finalizeLatch = new CountDownLatch(1)
  private val shutdownLatch = new CountDownLatch(1)

  private def ensurePartitionPlan(): Unit = {
    if (_partitionPlan.isEmpty) {
      println("⚠️  Waiting for PartitionPlan from Master...")

      val deadline = System.nanoTime() + 120_000_000_000L
      
      while (_partitionPlan.isEmpty && System.nanoTime() < deadline) {
        Thread.sleep(500)
      }

      if (_partitionPlan.isEmpty) {
        throw new RuntimeException(
          "Timeout waiting for PartitionPlan from Master!"
        )
      }
    }
  }

  def setWorkerInfo(info: WorkerInfo): Unit = {
    _workerInfo = Some(info)
  }

  def getWorkerInfo: Option[WorkerInfo] = _workerInfo

  def getWorkerId: Int = _workerInfo.map(_.id.toInt).getOrElse(-1)
  
  def getInputPaths: Seq[String] = _workerInfo.map(_.inputDirs).getOrElse(Seq.empty)

  def setMasterClient(client: MasterClient): Unit = {
    _masterClient = Some(client)
  }

  def getMasterClient: MasterClient = {
    _masterClient.getOrElse {
      throw new RuntimeException("MasterClient not set!")
    }
  }

  def setWorkerAddresses(addresses: Map[Int, (String, Int)]): Unit = {
    _workerAddresses = Some(addresses)
  }

  def getWorkerAddresses: Option[Map[Int, (String, Int)]] = _workerAddresses

  def setPartitionPlan(plan: PartitionPlan): Unit = {
    _partitionPlan = Some(plan)
  }

  def getPartitionTargetWorker(partitionId: Int): Int = {
    ensurePartitionPlan()
    
    _partitionPlan match {
      case Some(plan) if partitionId < plan.ranges.size =>
        plan.ranges(partitionId).targetWorker
      case Some(plan) =>
        throw new RuntimeException(s"Partition $partitionId out of range (max: ${plan.ranges.size - 1})")
      case None =>
        throw new RuntimeException("PartitionPlan reconstruction failed!")
    }
  }

  def getMyPartitions: Seq[Int] = {
    ensurePartitionPlan()
    
    val myId = getWorkerId
    _partitionPlan match {
      case Some(plan) =>
        plan.ranges.zipWithIndex
          .filter(_._1.targetWorker == myId)
          .map(_._2)
      case None =>
        throw new RuntimeException("PartitionPlan reconstruction failed!")
    }
  }

  def getTotalPartitions: Int = {
    ensurePartitionPlan()
    _partitionPlan.map(_.ranges.size).getOrElse(0)
  }

  def getSplitters: Array[Array[Byte]] = {
    ensurePartitionPlan()
    
    _partitionPlan match {
      case Some(plan) =>
        plan.ranges.dropRight(1).map(_.hi.toByteArray).toArray
      
      case None =>
        throw new RuntimeException("PartitionPlan not available!")
    }
  }

  def findPartitionId(key: Array[Byte]): Int = {
    val splitters = getSplitters
    var idx = 0
    while (idx < splitters.length && RecordIO.compareKeys(splitters(idx), key) < 0) {
      idx += 1
    }
    idx
  }

  def setShuffleReport(report: ShuffleCompletionReport): Unit = {
    _shuffleReport = Some(report)
  }

  def reportShuffleComplete(): Unit = {
    (_masterClient, _shuffleReport) match {
      case (Some(client), Some(report)) =>
        client.reportShuffleComplete(report)
      case (None, _) =>
        System.err.println("❌ MasterClient not set!")
      case (_, None) =>
        System.err.println("❌ Shuffle report not set!")
    }
  }

  def reportMergeComplete(): Unit = {
    _masterClient match {
      case Some(client) =>
        client.reportMergeComplete(getWorkerId)
      case None =>
        System.err.println("❌ MasterClient not set!")
    }
  }

  def signalFinalizeComplete(): Unit = {
    finalizeLatch.countDown()
  }

  def awaitFinalizeComplete(): Unit = {
    finalizeLatch.await()
  }

  def signalShutdown(): Unit = synchronized {
    if (!shutdownSignaled) {
      shutdownSignaled = true
      shutdownLatch.countDown()
    }
  }

  def awaitShutdownCommand(): Unit = {
    shutdownLatch.await()
  }
}