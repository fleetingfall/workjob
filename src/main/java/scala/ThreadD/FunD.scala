package scala.ThreadD

import java.net.{ServerSocket, Socket}
import java.time.LocalTime
import java.util.concurrent.{Callable, ExecutorService, Executors, FutureTask}

import org.junit.Test

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Future, Promise}
import scala.util.{Failure, Random, Success}

/**
  * future是用来获取同步操作结果的
  * Future提供了一套高效便捷的非阻塞并行操作管理方案。其基本思想很简单，所谓Future，指的是一类占位符对象，用于指代某些尚未完成的计算的结果
  * 由Future指代的计算都是并行执行的，计算完毕后可另行获取相关计算结果
  * _Future[T]_ 是表示future对象的类型
  */
object FunD  {
  def main(args: Array[String]): Unit = {
    /*hello.start()*/
    /*(new NetworkService2(2020, 2)).run*/
    FunDemo

  }
  def teDM(): Unit ={
    Future{
      Thread.sleep(1*1000)
      println("我等了1s")
    }
    println("我是主函数")
    Thread.sleep(10*1000)
  }

  def FunDemo(): Unit ={
    val f:Future[List[String]]=Future{
      session.getABC()
    }
    val f2:Future[List[String]]=Future{
      session.getABC()
    }
    /*
    * 在处理完成的情况下  对处理的结果进行处理
    * 也可以使用偏函数，仅仅对成功或失败的结果进行处理
    *f onFailure {
        case t => println("An error has occured: " + t.getMessage)
      }
      f onSuccess {
        case posts => for (post <- posts) println(post)
      }
    * */
    f onComplete {
      case Success(posts) => println(posts.mkString("\t"))
      case Failure(t) => println("An error has occured: " + t.getMessage)
    }
    f2 onComplete {
      case Success(posts) => println(posts.mkString("\t"))
      case Failure(t) => println("An error has occured: " + t.getMessage)
    }
    /*主线程不能提前结束，不然可能无法看到结果  也就是说上面的方法不是阻塞的*/
    Thread.sleep(3*1000)


  }

}

object session{
  val list=List("a","b","c","d","e","f","g","h","i","j","k")
  @Test
  def getABC(): List[String] ={
    list.take(new Random().nextInt(list.size))
  }
}


object Test {
  def main(args: Array[String]) {
    val threadPool:ExecutorService=Executors.newFixedThreadPool(3)
    try {
        val future=new FutureTask[String](new Callable[String] {
        override def call(): String = {
            return "im result"
        }})
        threadPool.execute(future)
        Thread.sleep(1000)
        println(future.get())
      }finally {
        threadPool.shutdown()
    }
  }
}

class NetworkService(port: Int, poolSize: Int) extends Runnable {
  val serverSocket = new ServerSocket(port)
  def run() {
    while (true) {
      // 这里会阻塞直到有连接进来
      val socket = serverSocket.accept()
      //(new Handler(socket)).run() 没有启动线程的 就是主线程
      (new Thread(new Handler(socket))).start()
    }
  }
}

class Handler(socket: Socket) extends Runnable {
  def message = (Thread.currentThread.getName() + "\n").getBytes

  def run() {
    socket.getOutputStream.write(message)
    socket.getOutputStream.close()
  }
}
class NetworkService2(port: Int, poolSize: Int) extends Runnable {
  val serverSocket = new ServerSocket(port)
  val pool: ExecutorService = Executors.newFixedThreadPool(poolSize)

  def run() {
    try {
      while (true) {
        // This will block until a connection comes in. 会发现用来用去就两个线程
        val socket = serverSocket.accept()
        pool.execute(new Handler2(socket))
      }
    } finally {
      pool.shutdown()
    }
  }
}
class Handler2(socket: Socket) extends Runnable {
  def message = (Thread.currentThread.getName() + "\n").getBytes
  def run() {
    socket.getOutputStream.write(message)
    socket.getOutputStream.write("没有关闭线程连接，看会不会可以持续开线程".getBytes())
  }
}


