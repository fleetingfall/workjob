package scala.ThreadD



import java.util.Random

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

object FunNonBlockingSuccessAndFailuer extends App {

  val f = Future {
    Thread.sleep(new Random().nextInt(500))
    if (new Random().nextInt(500) > 250) throw new Exception("Tikes!") else 42
  }

  f onSuccess {
    case result => println(s"Success: $result")
  }

  f onFailure {
    case t => println(s"Exception: ${t.getMessage}")
  }

  // do the rest of your work
  println("A ...")
  Thread.sleep(100)
  println("B ....")
  Thread.sleep(100)
  println("C ....")
  Thread.sleep(100)
  println("D ....")
  Thread.sleep(100)
  println("E ....")
  Thread.sleep(100)

  Thread.sleep(1000)

}
