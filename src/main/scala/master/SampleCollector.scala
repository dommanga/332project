package master

import scala.collection.mutable

/** Interface */
trait SampleCollector {
  def collectAllSamples(): Array[Array[Byte]]
}

class SampleCollectorImpl(expectedWorkers: Int) extends SampleCollector {

  // Sample buffer for each Worker: WorkerId -> List[Key]
  private val samplesByWorker = mutable.Map.empty[String, mutable.ArrayBuffer[Array[Byte]]]
  private val lock = new Object

  /** Add sample from specific Worker */
  def addSample(workerId: String, key: Array[Byte]): Unit = lock.synchronized {
    val buffer = samplesByWorker.getOrElseUpdate(workerId, mutable.ArrayBuffer.empty)
    buffer += key
  }

  /** Mark as completed of sending samples of specific Worker */
  def markWorkerComplete(workerId: String): Unit = lock.synchronized {
    println(s"Worker $workerId completed sending samples (${samplesByWorker(workerId).size} samples)")
  }

  /** Check all Workers have completed */
  def isAllWorkersComplete: Boolean = lock.synchronized {
    samplesByWorker.size >= expectedWorkers
  }

  /** All Samples List */
  override def collectAllSamples(): Array[Array[Byte]] = lock.synchronized {
    val allSamples = samplesByWorker.values.flatten.toArray
    println(s"Total samples collected: ${allSamples.length} from ${samplesByWorker.size} workers")
    allSamples
  }

  /** Print statistics */
  def printStats(): Unit = lock.synchronized {
    println("=" * 60)
    println("Sample Collection Statistics:")
    samplesByWorker.foreach { case (workerId, samples) =>
      println(s"  Worker $workerId: ${samples.size} samples")
    }
    println(s"  Total: ${samplesByWorker.values.map(_.size).sum} samples")
    println("=" * 60)
  }
}