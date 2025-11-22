package master

import rpc.sort.{PartitionRange, PartitionPlan, TaskId, WorkerAddress}
import com.google.protobuf.ByteString
import common.RecordIO

object PartitionPlanner {
  /**
   * Create partition plan with worker addresses for distributed shuffle
   * 
   * @param splitters Splitter keys for partitioning
   * @param numWorkers Number of workers
   * @param workerAddresses List of (workerId, ip, port) tuples
   * @return PartitionPlan with ranges and worker addresses
   */
  def createPlan(
    splitters: Array[Array[Byte]], 
    numWorkers: Int,
    workerAddresses: Seq[(Int, String, Int)] = Seq.empty
  ): PartitionPlan = {
    
    val ranges = (0 until numWorkers).map { i =>
      val lo: Array[Byte] =
        if (i == 0) Array.fill[Byte](RecordIO.KeySize)(0)
        else        splitters(i - 1)

      val hi: Array[Byte] =
        if (i < splitters.length) splitters(i)
        else                      Array.fill[Byte](RecordIO.KeySize)(0xFF.toByte)

      PartitionRange(
        lo = ByteString.copyFrom(lo),
        hi = ByteString.copyFrom(hi),
        targetWorker = i
      )
    }.toIndexedSeq

    // Convert worker addresses to proto messages
    val workers = workerAddresses.map { case (id, ip, port) =>
      WorkerAddress(
        workerId = id,
        ip = ip,
        port = port
      )
    }.toIndexedSeq

    PartitionPlan(
      task    = Some(TaskId("task-001")),
      ranges  = ranges,
      workers = workers
    )
  }
}