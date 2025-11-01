package master
import rpc.MasterApi

class DummyMaster extends MasterApi {
  override def registerWorker(id: String, ip: String): Boolean = {
    println(s"[Master] registerWorker id=$id ip=$ip")
    true
  }
  override def submitTask(taskId: String, filePath: String): Unit =
    println(s"[Master] submitTask taskId=$taskId file=$filePath")
}
