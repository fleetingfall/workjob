package scala.AkkaDM.helloworld

import akka.actor.{Actor, ActorRef, ActorSystem, Props}

/**
  * 带交互的
  */
object HelloWorld2 extends App {

  import Greeter._
  val system = ActorSystem("actor-demo-scala")

  /**
    * 注意一下这里的props和以往的不一样，而是一个方法，但是返回的是一个Props对象
    *
    * 下面是用同一个类创建的两个不同的对象
    *
    * 如果尝试运行该代码几次，可能会看到这些行的顺序是相反的。这种排序是 Akka actor 系统动态本质的另一个例子，其中处理各个消息时的顺序是不确定的
    */
  val bob = system.actorOf(props("Bob", "What are you doing"))
  val alice = system.actorOf(props("Alice", "Happy to meet you"))

  bob ! Greet(alice)
  alice ! Greet(bob)
  Thread sleep 1000

  object Greeter {
    case class Greet(peer: ActorRef)
    case object AskName
    case class TellName(name: String)
    def props(name: String, greeting: String) = Props(new Greeter(name, greeting))
  }

  class Greeter(myName: String, greeting: String) extends Actor {
    import Greeter._
    def receive = {
      case Greet(peer) => peer ! AskName
      case AskName => sender ! TellName(myName)
      case TellName(name) => println(s"$greeting, $name")
    }
  }

}

object HelloWorld2_Simple extends App {

  import Greeter._
  val system = ActorSystem("actor-demo-scala")

  /**
    * 注意一下这里的props和以往的不一样，而是一个方法，但是返回的是一个Props对象
    *
    * 下面是用同一个类创建的两个不同的对象
    *
    * 如果尝试运行该代码几次，可能会看到这些行的顺序是相反的。这种排序是 Akka actor 系统动态本质的另一个例子，其中处理各个消息时的顺序是不确定的
    */
  val bob = system.actorOf(Props(new Greeter("Bob", "What are you doing")))
  val alice = system.actorOf(Props(new Greeter("Alice", "Happy to meet you")))

  bob ! Greet(alice)
  alice ! Greet(bob)
  Thread sleep 1000

  object Greeter {
    case class Greet(peer: ActorRef)
    case object AskName
    case class TellName(name: String)
  }

  class Greeter(myName: String, greeting: String) extends Actor {
    import Greeter._
    def receive = {
      case Greet(peer) => peer ! AskName
      case AskName => sender ! TellName(myName)
      case TellName(name) => println(s"$greeting, $name")
    }
  }

}

