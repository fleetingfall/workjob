package scala.AkkaDM.helloworld

import akka.actor.{Actor, ActorSystem, PoisonPill, Props, Terminated}

object BetterHelloWorld extends App{
  val system = ActorSystem("HelloActors")
  system.actorOf(Props[BetterMaster], "master")
}

//这里用三个case class来声明三种消息类型
// case class有一个好处就是可以用在case语句中
case class BetterGreet(name: String)
case class BetterPraise(name: String)
case class BetterCelebrate(name: String, age: Int)



class BetterTalker extends Actor {

  def receive() = {
    case BetterGreet(name) => println(s"Hello $name")
    case BetterPraise(name) => println(s"$name, you're amazing")
    case BetterCelebrate(name, age) => println(s"Here's to another $age years, $name")
  }
}
//BetterMaster   向      BetterTalker发送消息

class BetterMaster extends Actor {

  val talker = context.actorOf(Props[BetterTalker], "talker")

  override def preStart {
    context.watch(talker)

    talker ! BetterGreet("Dante")
    talker ! BetterPraise("Winston")
    talker ! BetterCelebrate("Clare", 16)
    //发送一个毒丸，告诉actor已经结束了。因此后面发送的消息将不会被传递
    talker ! PoisonPill
    talker ! BetterGreet("Dante")
  }
  def receive = {
    case _ => println("我怎么会接收到消息呢")
    case Terminated(`talker`) => context.system.terminate()
  }

}
