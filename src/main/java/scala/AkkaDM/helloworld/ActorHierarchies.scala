package scala.AkkaDM.helloworld

/**
  * 要创建Actor，可以调用ActorSystem.actorOf()，它创建的Actor在guardian actor之下；
  * 接着可以调用context.actorOf()在刚才创建的Actor内生成Actor树。这些方法会返回新创建的Actor的引用。每个Actor都可以直接访问Actor Context来或得它自身、Parent以及所有Children的引用。
  *
  * 用户通过system创建的 Actor 都是在 /user下
  */

import akka.actor.{Actor, ActorSystem, Props}
case object Pingpang

class Father extends Actor{
  override def receive() = {
    case Pingpang => println(s"${self.path}")      //        akka://ActorSystem/user/father

  }
}
class Son extends Actor{
  override def receive() = {
    case Pingpang => println(s"${self.path}")      //        akka://ActorSystem/user/father

  }
}
object ActorHierarchies extends App {
  val system1 = ActorSystem("ActorSystem1") // Guardian System Actor(也就是一个系统) 当然可以存在多个系统
  val father1=system1.actorOf(Props[Father], "father")
  val son1=system1.actorOf(Props[Son],"son")
  val son12=system1.actorOf(Props[Son],"son12") //一个系统中名称要唯一
  father1 ! Pingpang
  son1 ! Pingpang
  son12 ! Pingpang

  val system2 = ActorSystem("ActorSystem2") // Guardian System Actor(也就是一个系统) 当然可以存在多个系统
  val father2=system2.actorOf(Props[Father], "father")
  val son2=system2.actorOf(Props[Son],"son")
  father2 ! Pingpang
  son2 ! Pingpang

  system1.terminate()
  system2.terminate()


}
