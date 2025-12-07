package master

import rpc.sort._
import java.time.Instant
import scala.collection.mutable

/** Worker state */
object WorkerPhase extends Enumeration {
  type WorkerPhase = Value
  val ALIVE, DEAD = Value
}
import WorkerPhase._

case class RegisteredWorker(
  id: Int,
  workerInfo: WorkerInfo,
  lastHeartbeat: Instant,
  phase: WorkerPhase = WorkerPhase.ALIVE
)

class WorkerRegistry(expectedWorkers: Int) {

  private val workers = mutable.Map.empty[String, RegisteredWorker]  // IP -> Worker
  private var nextId = 0

  private def keyOf(info: WorkerInfo): String =
    s"${info.ip}:${info.port}"

  /** Register worker - initially assigns temporary ID based on IP */
  def register(info: WorkerInfo): WorkerAssignment = synchronized {
    val key = keyOf(info)
    
    workers.get(key) match {
      case Some(w) if w.phase == DEAD =>
        println(s"🔄 Worker ${info.ip} rejoining with ID ${w.id}")
        val revived = w.copy(
          workerInfo = info.copy(id = w.id),
          lastHeartbeat = Instant.now(),
          phase = ALIVE
        )
        workers(key) = revived

        WorkerAssignment(
          success = true,
          message = s"Rejoined as worker ${w.id}",
          workerId = w.id,
          partitionIds = Seq.empty,
          assignedPort = revived.workerInfo.port
        )

      case Some(w) if w.phase == ALIVE =>
        WorkerAssignment(
          success = true,
          message = s"Already registered as worker ${w.id}",
          workerId = w.id,
          partitionIds = Seq.empty,
          assignedPort = w.workerInfo.port
        )
      
      case Some(w) =>
        // fallback – theoretically unreachable
        println(s"⚠ Unexpected phase for worker at ${info.ip}: ${w.phase}")
        WorkerAssignment(
          success = true,
          message = s"Registered as worker ${w.id} (unexpected phase)",
          workerId = w.id,
          partitionIds = Seq.empty,
          assignedPort = w.workerInfo.port
        )

      case None =>
        val newId = nextId
        nextId += 1

        val newWorker = RegisteredWorker(
          id = newId,
          workerInfo = info.copy(id = newId),
          lastHeartbeat = Instant.now(),
          phase = ALIVE
        )
        workers(key) = newWorker

        WorkerAssignment(
          success = true,
          message = s"Registered as worker $newId",
          workerId = newId,
          partitionIds = Seq.empty,
          assignedPort = newWorker.workerInfo.port
        )
    }
  }

  /** Update heartbeat */
  def updateHeartbeat(info: WorkerInfo): Ack = synchronized {
    val key = keyOf(info)
    workers.get(key) match {
      case Some(w) if w.id == info.id =>
        workers(key) = w.copy(lastHeartbeat = Instant.now(), phase = ALIVE)
        Ack(ok = true, msg = "OK")
      
      case _ =>
        Ack(ok = false, msg = s"Unknown worker: ${info.ip}:${info.port}")
    }
  }

  /** All workers */
  def getAllWorkers: Seq[RegisteredWorker] = synchronized {
    workers.values.toSeq
  }

  /** Only alive workers */
  def getAliveWorkers: Seq[RegisteredWorker] = synchronized {
    workers.values.filter(_.phase == ALIVE).toSeq
  }

  /** Mark dead by worker ID */
  def markDead(workerId: Int): Unit = synchronized {
    workers.collectFirst { case (k, w) if w.id == workerId => (k, w) }
      .foreach { case (key, w) =>
        workers(key) = w.copy(phase = DEAD)
      }
  }

  /**
   * Check dead workers and run callback
   */
  def pruneDeadWorkers(
      timeoutSeconds: Int = 5
    )(onDead: Int => Unit = _ => ()): Unit = synchronized {

    val now = Instant.now()

    workers.foreach { case (ip, w) =>
      val diff = java.time.Duration.between(w.lastHeartbeat, now).getSeconds

      if (diff > timeoutSeconds && w.phase != DEAD) {
        println(s"💀 Worker ${w.id} DEAD (no heartbeat for ${diff}s)")
        markDead(w.id)
        onDead(w.id)
      }
    }
  }

  def size: Int = synchronized { workers.size }
}