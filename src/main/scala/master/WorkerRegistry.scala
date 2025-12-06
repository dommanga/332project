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
  private var idsAssigned = false

  /** Register worker - initially assigns temporary ID based on IP */
  def register(info: WorkerInfo): WorkerAssignment = synchronized {
    val ip = info.ip
    
    val existingWorker = workers.get(ip)
    
    existingWorker match {
      case Some(w) if w.phase == DEAD =>
        // Rejoining worker - keep same ID
        println(s"🔄 Worker ${info.ip} rejoining with ID ${w.id}")
        workers(ip) = w.copy(
          workerInfo = info.withPort(w.workerInfo.port),
          lastHeartbeat = Instant.now(),
          phase = ALIVE
        )
        
        WorkerAssignment(
          success = true,
          message = s"Rejoined as worker ${w.id}",
          workerId = w.id,
          partitionIds = Seq.empty,
          assignedPort = w.workerInfo.port
        )
        
      case Some(w) if w.phase == ALIVE =>
        // Already registered and alive
        WorkerAssignment(
          success = true,
          message = s"Already registered as worker ${w.id}",
          workerId = w.id,
          partitionIds = Seq.empty,
          assignedPort = w.workerInfo.port
        )
        
      case None =>
        // New worker - assign temporary ID -1 until all workers connect
        val tempId = -1
        
        workers(ip) = RegisteredWorker(
          id = tempId,
          workerInfo = info,
          lastHeartbeat = Instant.now(),
          phase = ALIVE
        )
        
        // If all workers connected, assign final IDs based on IP ordering
        if (workers.size == expectedWorkers && !idsAssigned) {
          assignFinalIds()
        }
        
        val finalId = workers(ip).id
        val finalPort = workers(ip).workerInfo.port
        
        WorkerAssignment(
          success = true,
          message = s"Registered as worker $finalId",
          workerId = finalId,
          partitionIds = Seq.empty,
          assignedPort = finalPort
        )
      
      case Some(w) =>
        throw new IllegalStateException(s"Unknown phase: ${w.phase}")
    }
  }
  
  /**
   * Assign final IDs based on IP address ordering
   */
  private def assignFinalIds(): Unit = {
    val sortedWorkers = workers.toSeq.sortBy(_._1)  // Sort by IP
    
    sortedWorkers.zipWithIndex.foreach { case ((ip, worker), idx) =>
      workers(ip) = worker.copy(
        id = idx,
        workerInfo = worker.workerInfo
      )
    }
    
    idsAssigned = true
  }

  /** Update heartbeat */
  def updateHeartbeat(info: WorkerInfo): Ack = synchronized {
    workers.get(info.ip) match {
      case Some(w) if w.id == info.id =>
        workers(info.ip) = w.copy(lastHeartbeat = Instant.now(), phase = ALIVE)
        Ack(ok = true, msg = "OK")
      
      case _ =>
        Ack(ok = false, msg = s"Unknown worker: ${info.ip}")
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
    workers.values.find(_.id == workerId).foreach { w =>
      workers(w.workerInfo.ip) = w.copy(phase = DEAD)
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