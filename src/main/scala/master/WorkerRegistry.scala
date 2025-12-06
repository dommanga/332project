package master

import rpc.sort._
import java.time.Instant
import scala.collection.mutable

/** Worker state (Week7) */
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

class WorkerRegistry {

  private val workers = mutable.Map.empty[Int, RegisteredWorker]
  private var nextId = 0

  /** Register worker */
  def register(info: WorkerInfo): WorkerAssignment = synchronized {
    val existingWorker = workers.find { case (_, w) => 
      w.workerInfo.ip == info.ip
    }
    
    val workerId = existingWorker match {
      case Some((id, w)) if w.phase == DEAD =>
        println(s"[Registry] Worker at ${info.ip} rejoining with original ID $id")
        id
        
      case Some((id, w)) if w.phase == ALIVE =>
        println(s"[Registry] Worker at ${info.ip} re-registering with ID $id (was ALIVE)")
        id
        
      case None =>
        val id = nextId
        nextId += 1
        println(s"[Registry] New worker at ${info.ip} assigned ID $id")
        id
      case _ =>
        throw new RuntimeException("Unexpected state in worker registration")
    }

    val assignedPort = 8000 + workerId
    val updatedInfo = info.withPort(assignedPort)

    workers(workerId) = RegisteredWorker(
      id = workerId,
      workerInfo = updatedInfo,
      lastHeartbeat = Instant.now(),
      phase = ALIVE
    )

    WorkerAssignment(
      success = true,
      message = s"Registered as worker $workerId",
      workerId = workerId,
      partitionIds = Seq.empty,
      assignedPort = assignedPort
    )
  }

  /** Update heartbeat */
  def updateHeartbeat(info: WorkerInfo): Ack = synchronized {
    workers.get(info.id) match {
      case Some(w) =>
        workers(info.id) = w.copy(lastHeartbeat = Instant.now(), phase = ALIVE)
        Ack(ok = true, msg = s"Heartbeat updated for worker ${info.id}")
      
      case None =>
        Ack(ok = false, msg = s"Unknown worker: ${info.id}")
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

  /** Mark dead */
  def markDead(workerId: Int): Unit = synchronized {
    workers.get(workerId).foreach { w =>
      workers(workerId) = w.copy(phase = DEAD)
    }
  }

  /**
   * Check dead workers and run callback
   * callback(deadWorkerId) is executed for each dead worker
   */
  def pruneDeadWorkers(
      timeoutSeconds: Int = 10
    )(onDead: Int => Unit = _ => ()): Unit = synchronized {

    val now = Instant.now()

    workers.foreach { case (id, w) =>
      val diff = java.time.Duration.between(w.lastHeartbeat, now).getSeconds

      if (diff > timeoutSeconds && w.phase != DEAD) {
        Console.err.println(s"[Registry] Worker $id DEAD (no heartbeat for $diff s)")
        markDead(id)
        onDead(id)
      }
    }
  }

  def size: Int = synchronized { workers.size }
}
