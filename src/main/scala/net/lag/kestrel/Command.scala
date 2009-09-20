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

case class GetCommand(queueName: String, options: Options) extends Command

case class SetCommand(queueName: String, flags: Int, expiry: Int, data: Array[Byte]) extends Command {
  override def equals(obj: Any): Boolean = {
    if(obj.isInstanceOf[SetCommand]) {
      val cmd = obj.asInstanceOf[SetCommand]
      return (
        cmd.queueName == queueName && 
        cmd.flags == flags && 
        cmd.expiry == expiry &&
        cmd.data.length == data.length && cmd.data.zip(data).forall(a => a._1 == a._2)
      )
    } else {
      false
    }
  }
}

case class FlushCommand(queueName: String) extends Command
case class FlushExpiredCommand(queueName: String) extends Command
case class DeleteCommand(queueName: String) extends Command

case object StatsCommand extends Command
case object ShutdownCommand extends Command
case object ReloadCommand extends Command
case object FlushAllCommand extends Command
case object DumpConfigCommand extends Command
case object DumpStatsCommand extends Command
case object FlushAllExpiredCommand extends Command

case object VersionCommand extends Command
