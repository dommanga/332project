import org.scalatest.flatspec.AnyFlatSpec
import master.PartitionPlanner
import common.RecordIO
import com.google.protobuf.ByteString
import rpc.sort.{PartitionPlan, PartitionRange, TaskId}

class PartitionPlannerSpec extends AnyFlatSpec {

  private def arr(bytes: Int*): Array[Byte] =
    bytes.map(_.toByte).toArray.padTo(RecordIO.KeySize, 0.toByte)

  "PartitionPlanner" should "create k ranges from k-1 splitters with correct boundaries" in {
    val k = 3 // workers
    val s0 = arr(0x10)
    val s1 = arr(0xA0)
    val splitters = Array(s0, s1)

    val plan = PartitionPlanner.createPlan(splitters, k)

    assert(plan.task.nonEmpty && plan.task.get.id == "task-001")
    assert(plan.ranges.size == k)

    val r0 = plan.ranges(0) // [00..00, 10..00)
    val r1 = plan.ranges(1) // [10..00, A0..00)
    val r2 = plan.ranges(2) // [A0..00, FF..FF)

    def bytesOf(bs: ByteString) = bs.toByteArray

    // lo/hi 체크
    assert(bytesOf(r0.lo).sameElements(Array.fill[Byte](RecordIO.KeySize)(0)))
    assert(bytesOf(r0.hi).sameElements(s0))

    assert(bytesOf(r1.lo).sameElements(s0))
    assert(bytesOf(r1.hi).sameElements(s1))

    assert(bytesOf(r2.lo).sameElements(s1))
    assert(bytesOf(r2.hi).sameElements(Array.fill[Byte](RecordIO.KeySize)(0xFF.toByte)))

    // target worker id 연속성
    assert(plan.ranges.map(_.targetWorker) == Seq(0,1,2))
  }

  it should "work when there are no splitters (k=1)" in {
    val plan = PartitionPlanner.createPlan(Array.empty[Array[Byte]], numWorkers = 1)
    assert(plan.ranges.size == 1)
    val r = plan.ranges.head
    assert(r.targetWorker == 0)
    assert(r.lo.toByteArray.sameElements(Array.fill[Byte](RecordIO.KeySize)(0)))
    assert(r.hi.toByteArray.sameElements(Array.fill[Byte](RecordIO.KeySize)(0xFF.toByte)))
  }
}
