package scala.AkkaDM.wordcount

import akka.actor.{Actor, ActorRef, PoisonPill, Props, Terminated, actorRef2Scala}
import akka.routing.{Broadcast, RoundRobinPool}

class MasterActor extends Actor {

  val aggregateActor:ActorRef = context.actorOf(Props[AggregateActor],"aggregate")
  val reduceActor:ActorRef = context.actorOf(Props(new ReduceActor(aggregateActor)).withRouter(RoundRobinPool(4)),"reduce")
  val mapActor:ActorRef = context.actorOf(Props(new MapActor(reduceActor)),"map")


  def receive: Receive = {
    case message: String => mapActor ! message
  }



}