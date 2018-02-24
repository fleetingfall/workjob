package scala.AkkaDM.helloworld

import akka.actor.{Actor, ActorRef, ActorSystem, Props, ReceiveTimeout, Terminated}

/**
  * actor创建的两种方式  默认Props[]   手动Props(new Actorss())
  */
object Hello2 extends App {
  val system=ActorSystem("system")
  val actor2=system.actorOf(Props(new Actor2("kingcall")),"actor2")
 /*
 不能传入同一对象给  Props
  val obj=new Actor2("kingcall")
  val obj1=system.actorOf(Props(obj),"obj1")
  val obj2=system.actorOf(Props(obj),"obj2")
  */
  actor2 ! "test"
  val actor1=system.actorOf(Props[Actor1],"actor1")
  actor1 ! "test"



  Thread sleep(1000)
  system terminate()

}

/**
  * 没有构造方法的Actor
  */
class Actor1 extends Actor{
  override def receive={
    case "test" =>println("received test")
    case _ => println("receive unknown message")
  }
}

/**
  * 有构造方法的Actor
  * @param name
  */
class Actor2(name:String) extends Actor{
  override def receive={
    case "test" =>println("received test")
    case _ => println("receive unknown message")
  }
}

/**
  * 处理未能成匹配的消息
  * 监视和停止监视 Actor
  * 在被监视的 Actor 停止后,会收到 Terminated(other) 类型的消息
  * @param other
  */
class watchActor(val other:ActorRef) extends Actor{
  context.watch(other)
  //消息发给已经停止的 actor 会被转发到系统的 deadLetters
  var lastSernder=context.system.deadLetters

  override def receive={
    case "stop" => println("收到停止消息");context.stop(other)
    case Terminated(other)=>println("已结停止了");lastSernder ! other.toString()+"finished"
    case ReceiveTimeout => println("接收消息超时了")
  }

  /**
    * 对所有未能成功匹配的方法进行处理
    * @param message
    */
  override def unhandled(message: Any): Unit ={
    super.unhandled(message)
    println(message.toString)

  }
}


object test_watchActor extends App{
  val system=ActorSystem("system")
  val actor2=system.actorOf(Props(new Actor2("kingcall")),"actor2")
  val act=system.actorOf(Props(new watchActor(actor2)),"act")
  act ! "stop"
  act ! "test"
  Props
}

/**
  * 为了保证能够将 Actor 的实例封装起来，不让其被外部直接访问，我们将所有构造函数的参数传给一个 Props 的实例。
  * 如果 Actor 的构造函数有参数，那么推荐的做法是通过一个工厂方法来创建 Props。
  */

class ActorProps extends Actor{

  override def receive={
    case s => println(s"${self.path}"+":"+s)
  }
}

object aboutProps extends App {
  val system=ActorSystem("system")
  val actor1=system.actorOf(Props[ActorProps],"ActorProps")
  actor1 ! "info"
}



