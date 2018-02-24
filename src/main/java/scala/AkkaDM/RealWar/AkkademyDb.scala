package scala.AkkaDM.RealWar

import java.util

import akka.actor.Actor
import akka.event.Logging

case class SetRequest(key: String, value: Object)

class AkkademyDb extends Actor {
  val map = new util.HashMap[String, Object]
  val log = Logging(context.system, this)
  override def receive = {
    case SetRequest(key, value) => {
      log.info("received SetRequest - key: {} value: {}", key, value)
      map.put(key, value)
    }
    //通过变量完成通用的匹配
    case o => log.info("received unknown message: {}", o);
  }
}
