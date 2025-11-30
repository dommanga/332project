package master

import rpc.sort._
import com.google.protobuf.ByteString

object PartitionPlanner {

  /** Create PartitionPlan (Week7 version with worker addresses) */
  def createPlan(
      splitters: Seq[Array[Byte]],
      numWorkers: Int,
      workerAddresses: Seq[WorkerAddress]
  ): PartitionPlan = {

    val ranges = makeRanges(splitters, numWorkers)

    PartitionPlan(
      task = Some(TaskId("task-001")),
      ranges = ranges,
      workers = workerAddresses
    )
  }

  /** Convert splitters into PartitionRanges */
  private def makeRanges(
      splitters: Seq[Array[Byte]],
      numWorkers: Int
  ): Seq[PartitionRange] = {

      val fullMin: Array[Byte] = Array.fill(10)(0x00.toByte)
      val fullMax: Array[Byte] = Array.fill(10)(0xFF.toByte)

    // Bounds = [MIN, splitters..., MAX]
    val bounds = Seq(fullMin) ++ splitters ++ Seq(fullMax)

    // Sliding window → (lo, hi)
    bounds
      .sliding(2)
      .zipWithIndex
      .map { case (Seq(lo, hi), idx) =>
        PartitionRange(
          lo = ByteString.copyFrom(lo),
          hi = ByteString.copyFrom(hi),
          targetWorker = idx % numWorkers
        )
      }
      .toSeq
  }
}
