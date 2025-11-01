package worker
import rpc.WorkerApi

class DummyWorker extends WorkerApi {
  override def heartbeat(id: String): Unit =
    println(s"[Worker] heartbeat id=$id")
  override def runLocalSort(taskId: String, inputPath: String, outputPath: String): Unit =
    println(s"[Worker] runLocalSort taskId=$taskId in=$inputPath out=$outputPath")
}
