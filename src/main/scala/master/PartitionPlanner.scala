package master

import rpc.sort.{PartitionRange, PartitionPlan, TaskId}
import com.google.protobuf.ByteString
import common.RecordIO

object PartitionPlanner {
  def createPlan(splitters: Array[Array[Byte]], numWorkers: Int): PartitionPlan = {
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

    PartitionPlan(
      task   = Some(TaskId("task-001")),
      ranges = ranges
    )
  }
}
