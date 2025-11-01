package worker
object DemoHeartbeat extends App {
  val w = new DummyWorker
  println("== Demo: worker heartbeat ==")
  w.heartbeat("w-001")
}
