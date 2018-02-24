package scala.AkkaDM.helloworld

import akka.actor.{Actor, ActorSystem, Props}

import scala.AkkaDM.helloworld.ReplyDM.system

case class Hello(name: String)

/**
  * 如果消息不是来自Actor,sender默认就是deadLetters
  * 我们接着通过 sender()方法获取了发 送者的 ActorRef(或者发送者想让我们看到的ActorRef)。我们可以向该 ActorRef 发送消息，对发送者做出响应
  * 在 Actor内部中 ！ 默认自带发送者就是它自己
  *
  */
class Gretting extends Actor{

  override def receive()={
    case Hello(name)=>println(s"你好：${name}");sender ! Hello("kingcall")
  }
}

class Test extends Actor{

  override def receive()={
    case Hello(name)=>println(s"你好啊：${name}");sender ! Hello("king");
  }
}

/**
  * tell 方法在Actor内使用和不在Actor内使用的区别
  * tell 和 ！也不是完全相同的  例如 ！ 在外部是不需要ActorRef 的
  */
object ReplyDM extends App {
  val system=ActorSystem("system")
  val gretting=system.actorOf(Props[Gretting],"gretting")
  val test=system.actorOf(Props[Test],"test")
  test ! Hello("kingcall")
  test tell (Hello("kingcall"),gretting)

}
