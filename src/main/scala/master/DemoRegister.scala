package master
object DemoRegister extends App {
  val m = new DummyMaster
  println("== Demo: register worker ==")
  val ok = m.registerWorker("w-001", "127.0.0.1")
  println(s"register result=$ok")
}
