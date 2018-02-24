package scala.ThreadD

import scala.concurrent.Future
import scala.util.{Failure, Success}

/**
  * 带泛型的Futuer
  */

object GeneticFun extends App {
  import scala.concurrent.ExecutionContext.Implicits.global

  def longRunningComputation(i: Int): Future[Int] = Future {
    Thread.sleep(100)
    i + 1
  }

  longRunningComputation(11).onComplete {
    case Success(result) => println(s"result = $result")
    case Failure(e) => e.printStackTrace
  }

  // keep the jvm from shutting down
  Thread.sleep(1000)

}
