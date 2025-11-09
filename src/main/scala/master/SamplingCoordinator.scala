package master

import java.util.concurrent.atomic.AtomicInteger
import scala.collection.mutable
import common.RecordIO.compareKeys

// 인터페이스
trait SampleCollector {
  def expectedWorkers: Int
  def submit(workerId: Int, sampleKey: Array[Byte]): Unit
  def markCompleted(workerId: Int): Unit
  def allCompleted: Boolean
  def collectedSamples: Array[Array[Byte]]
}

trait SplitterCalculator {
  /** samples(정렬 전), numWorkers=k -> k-1개 splitter 반환 */
  def calculate(samples: Array[Array[Byte]], numWorkers: Int): Array[Array[Byte]]
}

// 수집 구현
final class InMemorySampleCollector(val expectedWorkers: Int) extends SampleCollector {
  private val bag = new mutable.ArrayBuffer[Array[Byte]](expectedWorkers * 1024)
  private val done = new mutable.BitSet(expectedWorkers)
  private val completedCnt = new AtomicInteger(0)

  override def submit(workerId: Int, sampleKey: Array[Byte]): Unit = this.synchronized {
    bag += sampleKey
  }
  override def markCompleted(workerId: Int): Unit = this.synchronized {
    if (!done.contains(workerId)) {
      done.add(workerId)
      completedCnt.incrementAndGet()
    }
  }
  override def allCompleted: Boolean = completedCnt.get() == expectedWorkers
  override def collectedSamples: Array[Array[Byte]] = this.synchronized { bag.toArray }
}

// 스플리터 계산
final class QuantileSplitterCalculator extends SplitterCalculator {
  override def calculate(samples: Array[Array[Byte]], numWorkers: Int): Array[Array[Byte]] = {
    require(numWorkers >= 2, "need at least 2 workers")
    if (samples.isEmpty) return Array.empty

    val sorted = samples.sortWith((a, b) => compareKeys(a, b) < 0)
    val k = numWorkers
    (1 until k).map { i =>
      val idx = math.min((i.toLong * sorted.length / k).toInt, sorted.length - 1)
      sorted(idx)
    }.toArray
  }
}

// 조정자: 일회 계산 보장
final class SamplingCoordinator(val expectedWorkers: Int) {
  private val collector = new InMemorySampleCollector(expectedWorkers)
  private val calc = new QuantileSplitterCalculator
  @volatile private var splittersOpt: Option[Array[Array[Byte]]] = None

  def submit(workerId: Int, key: Array[Byte]): Unit = collector.submit(workerId, key)

  def complete(workerId: Int): Unit = synchronized {
    collector.markCompleted(workerId)
    if (collector.allCompleted && splittersOpt.isEmpty) {
      splittersOpt = Some(calc.calculate(collector.collectedSamples, expectedWorkers))
    }
  }

  def isReady: Boolean = splittersOpt.nonEmpty
  def splitters: Array[Array[Byte]] = splittersOpt.getOrElse(Array.empty)
}
