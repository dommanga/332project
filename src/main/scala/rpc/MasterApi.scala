package rpc

trait MasterApi {
  def registerWorker(id: String, ip: String): Boolean
  def submitTask(taskId: String, filePath: String): Unit
}
