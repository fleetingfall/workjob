package scala.AkkaDM.helloworld

import akka.actor.{Actor, ActorSystem, Props}

/**
  * 带状态的
  */
object HelloWorld extends App {

  case class Greeting(greet: String)
  case class Greet(name: String)

  val system = ActorSystem("actor-demo-scala")
  //Props(new Hello) 什么时候用[]  什么时候用（）
  val hello = system.actorOf(Props[Hello], "hello")
  hello ! Greeting("Hello")
  hello ! Greet("Bob")
  hello ! Greet("Alice")
  hello ! Greeting("Hola")
  hello ! Greet("Alice")
  hello ! Greet("Bob")
  Thread sleep 1000

  class Hello extends Actor {
    //状态信息  在这里是一个变量，用来存储信息
    var greeting = ""
    def receive = {
      case Greeting(greet) => greeting = greet
      case Greet(name) => println(s"$greeting $name")
    }
  }
}