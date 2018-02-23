package scala.AkkaDM.wordcount

import akka.actor.{ActorSystem, PoisonPill, Props, actorRef2Scala}

object WordCount {

  def main(args: Array[String]) {
    val _system = ActorSystem("WordCount")
    val master = _system.actorOf(Props[MasterActor], name = "master")

    master ! "The quick brown fox tried to jump over the lazy dog and fell on the dog"
    master ! "Dog is man's best friend"
    master ! "Dog and Fox belong to the same family"
    master ! "Dog and Fox belong to the same family"
    master ! "Dog and Fox belong to the same family"
    master ! "Dog and Fox belong to the same family"
    master ! "Dog and Fox belong to the same family"

    Thread.sleep(500)
    sys.exit()
  }
}