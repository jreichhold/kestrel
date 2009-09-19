package net.lag.kestrel

abstract class Command

case class GetCommand(val queueName: String) extends Command
case class SetCommand(val queueName: String, val data: Array[Byte]) extends Command
