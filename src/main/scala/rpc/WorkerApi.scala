package rpc

trait WorkerApi {
  def heartbeat(id: String): Unit
  def runLocalSort(taskId: String, inputPath: String, outputPath: String): Unit
}
