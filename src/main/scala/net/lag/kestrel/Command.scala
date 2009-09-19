package net.lag.kestrel

case class Options(
  timeout:  Option[Int], 
  closing:  Boolean, 
  opening:  Boolean,
  aborting: Boolean,
  peeking:  Boolean
)

object NoOptions extends Options(None, false, false, false, false)

abstract class Command

case class GetCommand(name: String, options: Options) extends Command

case class SetCommand(name: String, flags: Int, expiry: Int, data: Array[Byte]) extends Command {
  override def equals(obj: Any): Boolean = {
    if(obj.isInstanceOf[SetCommand]) {
      val cmd = obj.asInstanceOf[SetCommand]
      return (
        cmd.name == name && 
        cmd.flags == flags && 
        cmd.expiry == expiry &&
        cmd.data.length == data.length && cmd.data.zip(data).forall(a => a._1 == a._2)
      )
    } else {
      false
    }
  }
}

case class FlushCommand(name: String) extends Command

case class OtherCommand(line: List[String]) extends Command
// case class SetCommand(val queueName: String, val data: Array[Byte]) extends Command