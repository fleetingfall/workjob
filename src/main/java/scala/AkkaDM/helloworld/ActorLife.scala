package scala.AkkaDM.helloworld

import akka.actor.AbstractActor.Receive
import akka.actor.{Actor, ActorSystem, PoisonPill, Props}

/**
  * 演示生命周期的一些方法
  */
class StartStopActor1 extends Actor {
  override def preStart(): Unit = {
    println("first started")
    context.actorOf(Props[StartStopActor2], "second")
  }
  override def postStop(): Unit = println("first stopped")

  override def receive: Receive = {
    case "stop" ⇒ context.stop(self)
  }
}

class StartStopActor2 extends Actor {
  override def preStart(): Unit = println("second started")
  override def postStop(): Unit = println("second stopped")

  // Actor.emptyBehavior 是一个有用的占位符
  // 在我们不想用这个actor处理任何信息是使用它
  override def receive: Receive = Actor.emptyBehavior
}

object ActorLife extends App {
  val system=ActorSystem("system")
  val first = system.actorOf(Props[StartStopActor1], "first")
  Thread.sleep(100)//睡眠时间要长一点，不然你看不到想要的结果————递归关闭
  println("===================================")
  first ! "stop"
  Thread sleep 100
  system terminate()
}

/**
  * Actor停止示例
  */
class ActorSTOP extends Actor{
  override def receive={
    case _ => println("我还没有停止")
  }

}

object test_ActorSTOP extends App{
  val system=ActorSystem("system")
  val stop =system.actorOf(Props[ActorSTOP],"stop")
  stop ! "stop"
  stop ! PoisonPill  //发送药丸停止actor
  stop ! "stop"
}

