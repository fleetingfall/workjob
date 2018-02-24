package scala.ThreadD

import java.util.Random

import scala.concurrent.Future
import scala.util.{Failure, Success}
import scala.concurrent.ExecutionContext.Implicits.global

/**
  * 有时你只需要监听Future的完成事件，对其进行响应，不是创建新的Future，而仅仅是产生副作用。通过onComplete,onSuccess,onFailure三个回调函数来异步执行Future任务
  */
object FunNonBlocking extends App {
  println("starting calculation ...")
  val f = Future {
    Thread.sleep(new Random().nextInt(500))
    42
  }

  println("before onComplete")
  /**
    * 在此看到了事件驱动的威力，
    */
  f.onComplete{
    case Success(value) => println(s"Got the callback, meaning = $value")
    case Failure(e) => e.printStackTrace
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
  Thread.sleep(2000)

}
