package net.lag.kestrel

object Protocol {

case class ItemData(flags: Int, data: Array[Byte]) {
  override def equals(obj: Any): Boolean = {
    obj match {
      case ItemData(_flags, _data) => (
        _flags == flags &&
        _data.length == data.length && _data.zip(data).forall(a => a._1 == a._2)
      )
      case _ => false
    }
  }
}

case class Options(
  timeout:  Option[Int], 
  closing:  Boolean, 
  opening:  Boolean,
  aborting: Boolean,
  peeking:  Boolean
)

object NoOptions extends Options(None, false, false, false, false)

abstract class Request

case class GetRequest(queueName: String, options: Options) extends Request

case class SetRequest(queueName: String, expiry: Int, data: ItemData) extends Request

case class FlushRequest(queueName: String) extends Request
case class FlushExpiredRequest(queueName: String) extends Request
case class DeleteRequest(queueName: String) extends Request

case object StatsRequest extends Request
case object ShutdownRequest extends Request
case object ReloadRequest extends Request
case object FlushAllRequest extends Request
case object DumpConfigRequest extends Request
case object DumpStatsRequest extends Request
case object FlushAllExpiredRequest extends Request

case object VersionRequest extends Request

}