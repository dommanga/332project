import org.scalatest.flatspec.AnyFlatSpec
import common.RecordIO.compareKeys

class KeyCompareSpec extends AnyFlatSpec {
  "compareKeys" should "compare unsigned lexicographically" in {
    val a = Array[Byte](0x01,0x02,0x03,0,0,0,0,0,0,0)
    val b = Array[Byte](0x01,0xFF.toByte,0x00,0,0,0,0,0,0,0)
    assert(compareKeys(a,b) < 0) // 0x02 < 0xFF
  }
}
