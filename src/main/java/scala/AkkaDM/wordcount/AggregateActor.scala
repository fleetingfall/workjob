package scala.AkkaDM.wordcount

import collection.mutable.HashMap
import akka.actor.Actor



class AggregateActor extends Actor {

  val wordCounts = new HashMap[String, Int]().withDefaultValue(0)

  def receive: Receive = {
    case message: ReduceData => aggregateInMemoryReduce(message.reduceDataMap)

  }

  def aggregateInMemoryReduce(reducedList: HashMap[String, Integer]) {
    var count: Integer = 0
    for (key <- reducedList.keySet) {
      if (wordCounts.contains(key)) {
        count = reducedList(key) + wordCounts(key)
        wordCounts.put(key, count)
      }
      else {
        wordCounts.put(key, reducedList(key))
      }
    }
  }

  override def postStop() {

    wordCounts.toList.sortBy(_._2).takeRight(10).map(println)

  }
}