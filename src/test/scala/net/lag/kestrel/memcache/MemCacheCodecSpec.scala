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

import org.apache.mina.core.buffer.IoBuffer
import org.apache.mina.core.filterchain.IoFilter
import org.apache.mina.core.session.{DummySession, IoSession}
import org.apache.mina.filter.codec._
import org.specs._

import net.lag.kestrel.{Command, GetCommand, OtherCommand}

object MemCacheCodecSpec extends Specification {

  private val fakeSession = new DummySession

  private val fakeDecoderOutput = new ProtocolDecoderOutput {
    override def flush(nextFilter: IoFilter.NextFilter, s: IoSession) = {}
    override def write(obj: AnyRef) = {
      written = obj :: written
    }
  }

  private var written: List[AnyRef] = Nil


  "Memcache Decoder" should {
    doBefore {
      written = Nil
    }

    def doDecode(s:String): Unit = memcache.Codec.decoder.decode(fakeSession, IoBuffer.wrap(s.getBytes), fakeDecoderOutput)

    "'get' request chunked various ways" in {
      doDecode("get foo\r\n")
      written mustEqual List(GetCommand("foo", NoOptions))
      written = Nil

      doDecode("get f")
      written mustEqual Nil
      doDecode("oo\r\n")
      written mustEqual List(GetCommand("foo", NoOptions))
      written = Nil

      doDecode("g")
      written mustEqual Nil
      doDecode("et foo\r")
      written mustEqual Nil
      doDecode("\nget ")
      written mustEqual List(GetCommand("foo", NoOptions))
      doDecode("bar\r\n")
      written mustEqual List(GetCommand("bar", NoOptions), GetCommand("foo", NoOptions))
    }

    "'get' with options" in {
      doDecode("get foo/t=5/close/open\r\n")
      written mustEqual List(GetCommand("foo", Options(Some(5), true, true, false, false)))
    }
    
    "'set' request chunked various ways" in {
      def bytes(s:String) = s.getBytes("ISO-8859-1")
      
      doDecode("set foo 0 0 5\r\nhello\r\n")
      written mustEqual List(SetCommand("foo", 0, 0, bytes("hello")))
      written = Nil

      doDecode("set foo 0 0 5\r\n")
      written mustEqual Nil
      doDecode("hello\r\n")
      written mustEqual List(SetCommand("foo", 0, 0, bytes("hello")))
      written = Nil

      doDecode("set foo 0 0 5")
      written mustEqual Nil
      doDecode("\r\nhell")
      written mustEqual Nil
      doDecode("o\r\n")
      written mustEqual List(SetCommand("foo", 0, 0, bytes("hello")))
      written = Nil
    }
    
    "'flush' command" in {
      doDecode("flush foo\r\n")
      written mustEqual List(FlushCommand("foo"))
    }
    
    "'delete' command" in {
      doDecode("delete foo\r\n")
      written mustEqual List(DeleteCommand("foo"))
    }
    
  }
}
