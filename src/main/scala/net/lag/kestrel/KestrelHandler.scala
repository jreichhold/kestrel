/*
 * Copyright 2009 Twitter, Inc.
 * Copyright 2009 Robey Pointer <robeypointer@gmail.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License. You may obtain
 * a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package net.lag.kestrel

import java.io.IOException
import java.net.InetSocketAddress
import java.nio.ByteOrder
import scala.actors.Actor
import scala.actors.Actor._
import scala.collection.mutable
import net.lag.configgy.{Config, Configgy, RuntimeEnvironment}
import net.lag.logging.Logger
import net.lag.naggati.{IoHandlerActorAdapter, MinaMessage, ProtocolError}
import org.apache.mina.core.buffer.IoBuffer
import org.apache.mina.core.session.{IdleStatus, IoSession}
import org.apache.mina.transport.socket.SocketSessionConfig

import Protocol._

class KestrelHandler(val session: IoSession, val config: Config) extends Actor {
  private val log = Logger.get

  private val IDLE_TIMEOUT = 60
  private val sessionID = KestrelStats.sessionID.incr
  private val remoteAddress = session.getRemoteAddress.asInstanceOf[InetSocketAddress]

  private var pendingTransaction: Option[(String, Int)] = None

  // used internally to indicate a client error: tried to close a transaction on the wrong queue.
  private class MismatchedQueueException extends Exception


  session.getConfig.setReadBufferSize(2048)
  IoHandlerActorAdapter.filter(session) -= MinaMessage.SessionOpened
  IoHandlerActorAdapter.filter(session) -= classOf[MinaMessage.MessageSent]

  // config can be null in unit tests
  val idleTimeout = if (config == null) IDLE_TIMEOUT else config.getInt("timeout", IDLE_TIMEOUT)
  if (idleTimeout > 0) {
    session.getConfig.setIdleTime(IdleStatus.BOTH_IDLE, idleTimeout)
  }

  KestrelStats.sessions.incr
  KestrelStats.totalConnections.incr
  log.debug("New session %d from %s:%d", sessionID, remoteAddress.getHostName, remoteAddress.getPort)
  start

  def act = {
    loop {
      react {
        case MinaMessage.MessageReceived(msg) =>
          handle(msg.asInstanceOf[Request])

        case MinaMessage.ExceptionCaught(cause) =>
          cause.getCause match {
            case _: ProtocolError => writeResponse("CLIENT_ERROR\r\n")
            case _: IOException =>
              log.debug("I/O Exception on session %d: %s", sessionID, cause.getMessage)
            case _ =>
              log.error(cause, "Exception caught on session %d: %s", sessionID, cause.getMessage)
              writeResponse("ERROR\r\n")
          }
          session.close

        case MinaMessage.SessionClosed =>
          log.debug("End of session %d", sessionID)
          abortAnyTransaction
          KestrelStats.sessions.decr
          exit()

        case MinaMessage.SessionIdle(status) =>
          log.debug("Idle timeout on session %s", session)
          session.close
      }
    }
  }

  private def writeResponse(out: String) = {
    val bytes = out.getBytes
    session.write(new memcache.Response(IoBuffer.wrap(bytes)))
  }

  private def writeResponse(out: String, data: Array[Byte]) = {
    val bytes = out.getBytes
    val buffer = IoBuffer.allocate(bytes.length + data.length + 7)
    buffer.put(bytes)
    buffer.put(data)
    buffer.put("\r\nEND\r\n".getBytes)
    buffer.flip
    KestrelStats.bytesWritten.incr(buffer.capacity)
    session.write(new memcache.Response(buffer))
  }

  private def handle(request: Request) = {
    request match {
      case GetRequest(queueName, options) => get(queueName, options)
      case SetRequest(queueName, expiry, data) => set(queueName, expiry, data)
      case DeleteRequest(queueName) => delete(queueName)
      case StatsRequest => stats
      case ShutdownRequest => shutdown

      case ReloadRequest =>
      	Configgy.reload
      	writeResponse("Reloaded config.\r\n")

      case FlushRequest(queueName) => flush(queueName)
      case FlushAllRequest =>
    	  for (qName <- Kestrel.queues.queueNames) {
      	  Kestrel.queues.flush(qName)
      	}
    	  writeResponse("Flushed all queues.\r\n")

      case FlushExpiredRequest(queueName) => flushExpired(queueName)
      case FlushAllExpiredRequest =>
    	  val flushed = Kestrel.queues.queueNames.foldLeft(0) { (sum, qName) => sum + Kestrel.queues.flushExpired(qName) }
    	  writeResponse("%d\r\n".format(flushed))

      case DumpConfigRequest => dumpConfig()
	    case DumpStatsRequest => dumpStats()

	    case VersionRequest => version()
    }
  }

  private def get(name: String, options: Options): Unit = {
    val timeout = options.timeout.getOrElse(0)

    log.debug("get -> q=%s t=%d open=%s close=%s abort=%s peek=%s",
              name, timeout, options.opening, options.closing, options.aborting, options.peeking)

    try {
      if (options.aborting) {
        if (!abortTransaction(name)) {
          log.warning("Attempt to abort a non-existent transaction on '%s' (sid %d, %s:%d)",
                      name, sessionID, remoteAddress.getHostName, remoteAddress.getPort)
        }
        writeResponse("END\r\n")
      } else {
        if (options.closing) {
          if (!closeTransaction(name)) {
            log.warning("Attempt to close a non-existent transaction on '%s' (sid %d, %s:%d)",
                        name, sessionID, remoteAddress.getHostName, remoteAddress.getPort)
            // let the client continue. it may be optimistically closing previous transactions as
            // it randomly jumps servers.
          }
          if (!options.opening) writeResponse("END\r\n")
        }
        if (options.opening || !options.closing) {
          if (pendingTransaction.isDefined && !options.peeking) {
            log.warning("Attempt to perform a non-transactional fetch with an open transaction on " +
                        " '%s' (sid %d, %s:%d)", name, sessionID, remoteAddress.getHostName,
                        remoteAddress.getPort)
            writeResponse("ERROR\r\n")
            session.close
            return
          }
          if (options.peeking) {
            KestrelStats.peekRequests.incr
          } else {
            KestrelStats.getRequests.incr
          }
          Kestrel.queues.remove(name, timeout, options.opening, options.peeking) {
            case None =>
              writeResponse("END\r\n")
            case Some(item) =>
              log.debug("get <- %s", item)
              if (options.opening) pendingTransaction = Some((name, item.xid))
              writeResponse("VALUE %s 0 %d\r\n".format(name, item.data.length), item.data)
          }
        }
      }
    } catch {
      case e: MismatchedQueueException =>
        log.warning("Attempt to close a transaction on the wrong queue '%s' (sid %d, %s:%d)",
                    name, sessionID, remoteAddress.getHostName, remoteAddress.getPort)
        writeResponse("ERROR\r\n")
        session.close
    }
  }

  // returns true if a transaction was actually closed.
  private def closeTransaction(name: String): Boolean = {
    pendingTransaction match {
      case None => false
      case Some((qname, xid)) =>
        if (qname != name) {
          throw new MismatchedQueueException
        } else {
          Kestrel.queues.confirmRemove(qname, xid)
          pendingTransaction = None
        }
        true
    }
  }

  private def abortTransaction(name: String): Boolean = {
    pendingTransaction match {
      case None => false
      case Some((qname, xid)) =>
        if (qname != name) {
          throw new MismatchedQueueException
        } else {
          Kestrel.queues.unremove(qname, xid)
          pendingTransaction = None
        }
        true
    }
  }

  private def abortAnyTransaction() = {
    pendingTransaction map { case (qname, xid) => Kestrel.queues.unremove(qname, xid) }
    pendingTransaction = None
  }

  private def set(name: String, expiry: Int, item: ItemData) = {
    log.debug("set -> q=%s expiry=%d flags=%d size=%d", name, expiry, item.flags, item.data.length)
    KestrelStats.setRequests.incr
    if (Kestrel.queues.add(name, item, expiry)) {
      writeResponse("STORED\r\n")
    } else {
      writeResponse("NOT_STORED\r\n")
    }
  }

  private def flush(name: String) = {
    log.debug("flush -> q=%s", name)
    Kestrel.queues.flush(name)
    writeResponse("END\r\n")
  }

  private def delete(name: String) = {
    log.debug("delete -> q=%s", name)
    Kestrel.queues.delete(name)
    writeResponse("END\r\n")
  }

  private def flushExpired(name: String) = {
    log.debug("flush_expired -> q=%s", name)
    writeResponse("%d\r\n".format(Kestrel.queues.flushExpired(name)))
  }

  private def stats() = {
    var report = new mutable.ArrayBuffer[(String, String)]
    report += (("uptime", Kestrel.uptime.toString))
    report += (("time", (Time.now / 1000).toString))
    report += (("version", Kestrel.runtime.jarVersion))
    report += (("curr_items", Kestrel.queues.currentItems.toString))
    report += (("total_items", Kestrel.queues.totalAdded.toString))
    report += (("bytes", Kestrel.queues.currentBytes.toString))
    report += (("curr_connections", KestrelStats.sessions.toString))
    report += (("total_connections", KestrelStats.totalConnections.toString))
    report += (("cmd_get", KestrelStats.getRequests.toString))
    report += (("cmd_set", KestrelStats.setRequests.toString))
    report += (("cmd_peek", KestrelStats.peekRequests.toString))
    report += (("get_hits", Kestrel.queues.queueHits.toString))
    report += (("get_misses", Kestrel.queues.queueMisses.toString))
    report += (("bytes_read", KestrelStats.bytesRead.toString))
    report += (("bytes_written", KestrelStats.bytesWritten.toString))

    for (qName <- Kestrel.queues.queueNames) {
      report ++= Kestrel.queues.stats(qName).map { case (k, v) => ("queue_" + qName + "_" + k, v) }
    }

    val summary = {
      for ((key, value) <- report) yield "STAT %s %s".format(key, value)
    }.mkString("", "\r\n", "\r\nEND\r\n")
    writeResponse(summary)
  }

  private def dumpConfig() = {
    val dump = new mutable.ListBuffer[String]
    for (qName <- Kestrel.queues.queueNames) {
      dump += "queue '" + qName + "' {"
      dump += Kestrel.queues.dumpConfig(qName).mkString("  ", "\r\n  ", "")
      dump += "}"
    }
    writeResponse(dump.mkString("", "\r\n", "\r\nEND\r\n"))
  }

  private def dumpStats() = {
    val dump = new mutable.ListBuffer[String]
    for (qName <- Kestrel.queues.queueNames) {
      dump += "queue '" + qName + "' {"
      dump += Kestrel.queues.stats(qName).map { case (k, v) => k + "=" + v }.mkString("  ", "\r\n  ", "")
      dump += "}"
    }
    writeResponse(dump.mkString("", "\r\n", "\r\nEND\r\n"))
  }

  private def version() = {
    writeResponse("VERSION " + Kestrel.runtime.jarVersion + "\r\n")
  }

  private def shutdown() = {
    Kestrel.shutdown
  }
}
