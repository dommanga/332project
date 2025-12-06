package worker

import scala.collection.mutable
import java.util.concurrent.CountDownLatch
import rpc.sort._

object WorkerState {
  @volatile private var _masterClient: Option[MasterClient] = None
  @volatile private var _workerInfo: Option[WorkerInfo] = None
  @volatile private var _workerAddresses: Option[Map[Int, (String, Int)]] = None
  @volatile private var _partitionPlan: Option[PartitionPlan] = None
  
  private var _shuffleReport: Option[ShuffleCompletionReport] = None
  private val finalizeLatch = new CountDownLatch(1)

  /**
   * PartitionPlan이 없으면 자동으로 재생성
   */
  private def ensurePartitionPlan(): Unit = {
    if (_partitionPlan.isEmpty) {
      println("[WorkerState] ⚠️ PartitionPlan missing, reconstructing...")
      
      _workerAddresses match {
        case Some(addresses) =>
          val numWorkers = addresses.size
          val numPartitions = numWorkers * 4
          
          // PartitionPlan 재구성
          val partitionsPerWorker = (numPartitions + numWorkers - 1) / numWorkers
          
          val ranges = (0 until numPartitions).map { pid =>
            val targetWorker = pid / partitionsPerWorker
            
            PartitionRange(
              lo = com.google.protobuf.ByteString.EMPTY,
              hi = com.google.protobuf.ByteString.EMPTY,
              targetWorker = targetWorker
            )
          }
          
          val workers = addresses.map { case (id, (ip, port)) =>
            WorkerAddress(workerId = id, ip = ip, port = port)
          }.toSeq
          
          val reconstructedPlan = PartitionPlan(
            task = Some(TaskId("recovered")),
            ranges = ranges,
            workers = workers
          )
          
          _partitionPlan = Some(reconstructedPlan)
          println(s"[WorkerState] ✅ Reconstructed PartitionPlan: $numPartitions partitions, $numWorkers workers")
          
        case None =>
          throw new RuntimeException("Cannot reconstruct PartitionPlan: no worker addresses!")
      }
    }
  }

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

  def setPartitionPlan(plan: PartitionPlan): Unit = {
    _partitionPlan = Some(plan)
    println(s"[WorkerState] Stored partition plan with ${plan.ranges.size} ranges")
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
        // ranges에서 hi 값 추출 (마지막 range 제외)
        plan.ranges.dropRight(1).map(_.hi.toByteArray).toArray
      
      case None =>
        throw new RuntimeException("PartitionPlan not available - cannot extract splitters!")
    }
  }

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