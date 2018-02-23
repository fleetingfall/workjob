package scala.AkkaDM.wordcount

import java.util.StringTokenizer

import akka.actor.actorRef2Scala
import akka.actor.Actor
import akka.actor.ActorRef

import scala.collection.mutable.ArrayBuffer


class MapActor(reduceActor: ActorRef) extends Actor {

  def receive: Receive = {
    case message: String => reduceActor ! splitLine(message)
    case _ =>

  }
  // 把一句话切割，返回（word:1）
  def splitLine(line: String): MapData = {
    var dataList = new ArrayBuffer[Word]
    var parser: StringTokenizer = new StringTokenizer(line)
    while (parser.hasMoreTokens()) {
      var word: String = parser.nextToken().toLowerCase()
      dataList.append(new Word(word, 1))
    }
    return new MapData(dataList)
  }
}