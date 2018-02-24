package scala.AkkaDM.helloworld

import akka.actor.{Actor, ActorSystem, Props}
import akka.util.Timeout
import scala.concurrent.duration._

import scala.concurrent.Await

/**
  *
  *   阻塞方式（Blocking）：该方式下，父actor或主程序停止执行知道所有future完成各自任务。通过scala.concurrent.Await使用。
  *   非阻塞方式（Non-Blocking），也称为回调方式（Callback）：父actor或主程序在执行期间启动future，future任务和父actor并行执行，当每个future完成任务，
  * 将通知父actor。通过onComplete、onSuccess、onFailure方式使用。
  *
  *   通常有两种方法来从一个Actor获取回应: 第一种是发送一个消息actor ! msg，这种方法只在发送者是一个Actor时有效；第二种是通过一个Future。
  */

class FunActor extends Actor{
  override def receive={
    case a => println(a);sender ! "hello kingcall"
  }

}
object FutuerDM extends App {

  implicit val timeout = Timeout(5 seconds)
  val system=ActorSystem("system")
  val actor=system.actorOf(Props[FunActor],"funcator")
 /* val future = actor tell ( "king",Actor.noSender)
  val result = Await.result(future, timeout.duration).asInstanceOf[String]*/


}
