package master

import rpc.sort._
import java.time.Instant
import scala.collection.mutable

case class RegisteredWorker(
                             id: Int,
                             workerInfo: WorkerInfo,
                             lastHeartbeat: Instant
                           )

class WorkerRegistry {
  private val workers = mutable.Map.empty[Int, RegisteredWorker]
  private var nextId = 0

  /** Worker registration - Return WorkerAssignment */
  def register(info: WorkerInfo): WorkerAssignment = synchronized {
    val workerId = nextId
    nextId += 1

    workers(workerId) = RegisteredWorker(
      id = workerId,
      workerInfo = info,
      lastHeartbeat = Instant.now()
    )

    println(s"✅ Worker registered:")
    println(s"   id=$workerId, ip=${info.ip}:${info.port}")
    println(s"   inputs=${info.inputDirs}")
    println(s"   output=${info.outputDir}")

    // Create WorkerAssignment of Proto
    WorkerAssignment(
      success = true,
      message = s"Registered as worker $workerId",
      workerId = workerId,
      partitionIds = Seq.empty  // Empty in Week4
    )
  }

  /** Update Heartbeat - Find with WorkerInfo.id */
  def updateHeartbeat(info: WorkerInfo): Ack = synchronized {
    workers.find(_._2.workerInfo.id == info.id) match {
      case Some((workerId, registered)) =>
        workers(workerId) = registered.copy(lastHeartbeat = Instant.now())
        Ack(
          ok = true,
          msg = s"Heartbeat updated for worker ${info.id}"
        )

      case None =>
        Console.err.println(s"⚠️  Unknown worker heartbeat: ${info.id}")
        Ack(
          ok = false,
          msg = s"Unknown worker: ${info.id}"
        )
    }
  }

  /** Get info of all workers */
  def getAllWorkers: Seq[RegisteredWorker] = synchronized {
    workers.values.toSeq
  }

  /** num of Worker */
  def size: Int = synchronized {
    workers.size
  }

  /** Remove dead Worker (timeout: 30s) */
  def pruneDeadWorkers(timeoutSeconds: Int = 30): Unit = synchronized {
    val now = Instant.now()
    val dead = workers.filter { case (id, reg) =>
      java.time.Duration.between(reg.lastHeartbeat, now).getSeconds > timeoutSeconds
    }

    dead.foreach { case (id, reg) =>
      workers.remove(id)
      Console.err.println(s"Worker timeout: id=$id, ${reg.workerInfo.id}")
    }
  }
}