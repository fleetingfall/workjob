package scala.ThreadD

import scala.concurrent.{Await, Future}

import scala.concurrent.duration._

/**
  * 被传递给Future的代码块会被缺省的Dispatcher所执行，代码块的返回结果会被用来完成Future
  */
object FutureBlockDemo extends App{
  import scala.concurrent.ExecutionContext.Implicits.global

  // create a Future
  val f = Future {
    Thread.sleep(500)
    1+1
  }
  // this is blocking(blocking is bad)
  val result = Await.result(f, 1 second)
  // 如果Future没有在Await规定的时间里返回,
  // 将抛出java.util.concurrent.TimeoutException
  println(result)
  Thread.sleep(1000)
}
