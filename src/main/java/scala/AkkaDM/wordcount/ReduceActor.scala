package scala.AkkaDM.wordcount

import collection.mutable.{ArrayBuffer, HashMap}
import akka.actor.actorRef2Scala
import akka.actor.Actor
import akka.actor.ActorRef

class ReduceActor(aggregateActor: ActorRef) extends Actor {

  def receive: Receive = {
    case message: MapData => aggregateActor ! reduce(message.dataList)
    case _ =>
  }

  def reduce(dataList: ArrayBuffer[Word]): ReduceData = {

    val reducedMap = new HashMap[String, Integer]
    for (wc: Word <- dataList) {
      val word: String = wc.word
      if (reducedMap.contains(word)) {
        reducedMap.put(word, reducedMap(word)+1 )
      } else {
        reducedMap.put(word, 1)
      }
    }
    return new ReduceData(reducedMap)
  }
}