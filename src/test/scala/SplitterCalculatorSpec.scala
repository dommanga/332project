import org.scalatest.flatspec.AnyFlatSpec
import master.QuantileSplitterCalculator
import common.RecordIO
import common.RecordIO.compareKeys

class SplitterCalculatorSpec extends AnyFlatSpec {
  "QuantileSplitterCalculator" should "return k-1 splitters in nondecreasing order" in {
    val calc = new QuantileSplitterCalculator
    // 0..99를 키 마지막 바이트로 가진 샘플 100개
    val samples = (0 until 100).map { i =>
      val k = Array.fill[Byte](RecordIO.KeySize)(0)
      k(9) = i.toByte
      k
    }.toArray

    val splitters = calc.calculate(samples, numWorkers = 4) // 3개 기대
    assert(splitters.length == 3)
    assert(compareKeys(splitters(0), splitters(1)) <= 0)
    assert(compareKeys(splitters(1), splitters(2)) <= 0)
  }
}
