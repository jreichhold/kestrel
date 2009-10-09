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

package net.lag.kestrel.memcache

import scala.collection.mutable
import org.apache.mina.core.buffer.IoBuffer
import org.apache.mina.core.session.{IdleStatus, IoSession}
import org.apache.mina.filter.codec._
import net.lag.extensions._
import net.lag.naggati.{Decoder, End, ProtocolError, Step}
import net.lag.naggati.Steps._

import net.lag.kestrel.Protocol._

case class Response(data: IoBuffer)


object Codec {
  val encoder = new ProtocolEncoder {
    def encode(session: IoSession, message: AnyRef, out: ProtocolEncoderOutput) = {
      val buffer = message.asInstanceOf[Response].data
      KestrelStats.bytesWritten.incr(buffer.remaining)
      out.write(buffer)
    }

    def dispose(session: IoSession): Unit = {
      // nothing.
    }
  }

  val decoder = new Decoder(decodeStep)

  val decodeStep = readLine(true, "ISO-8859-1") { line =>
    KestrelStats.bytesRead.incr(line.length + 1)
    val segments = line.split(" ")
    segments(0) = segments(0).toUpperCase

    val command = segments(0)

    // temporarily make eclipse feel sane
    def ret(cmd: Protocol.Request) = {
      state.out.write(cmd)
      End
    }

    command match {
      case "GET" =>
        val name = segments(1)

        var key = name
        var timeout:Option[Int] = None
        var closing = false
        var opening = false
        var aborting = false
        var peeking = false

        if (name contains '/') {
          val options = name.split("/")
          key = options(0)
          for (i <- 1 until options.length) {
            val opt = options(i)
            if (opt startsWith "t=") {
              timeout = Some(opt.substring(2).toInt)
            }
            if (opt == "close") closing = true
            if (opt == "open") opening = true
            if (opt == "abort") aborting = true
            if (opt == "peek") peeking = true
          }
        }

        if ((key.length == 0) || ((peeking || aborting) && (opening || closing)) || (peeking && aborting)) {
          throw new ProtocolError("bad request: " + line)
        }

        ret(GetRequest(key, Options(timeout, closing, opening, aborting, peeking)))

      case "SET" =>
      	if (segments.length < 5) {
      	  throw new ProtocolError("Malformed request line")
      	}
      	val dataBytes = segments(4).toInt
      	readBytes(dataBytes + 2) {
      	  KestrelStats.bytesRead.incr(dataBytes + 2)
      	  // final 2 bytes are just "\r\n" mandated by protocol.
      	  val bytes = new Array[Byte](dataBytes)
      	  state.buffer.get(bytes)
      	  state.buffer.position(state.buffer.position + 2)

      	  try {
      		  ret(SetRequest(segments(1), segments(3).toInt, ItemData(segments(2).toInt, bytes)))
      	  } catch {
      	    case e: NumberFormatException =>
      	      throw new ProtocolError("bad request: " + line)
      	  }
      	}

      case "FLUSH" => ret(FlushRequest(segments(1)))
      case "FLUSH_EXPIRED" => ret(FlushExpiredRequest(segments(1)))
      case "DELETE" => ret(DeleteRequest(segments(1)))

      case "STATS" => ret(StatsRequest)
      case "SHUTDOWN" => ret(ShutdownRequest)
      case "RELOAD" => ret(ReloadRequest)
      case "FLUSH_ALL" => ret(FlushAllRequest)
      case "DUMP_CONFIG" => ret(DumpConfigRequest)
      case "DUMP_STATS" => ret(DumpStatsRequest)
      case "FLUSH_ALL_EXPIRED" => ret(FlushAllExpiredRequest)
      case "VERSION" => ret(VersionRequest)

      case _ => throw new ProtocolError("Invalid Request: " + command)
    }
  }
}
