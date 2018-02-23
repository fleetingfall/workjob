package scala.AkkaDM.helloworld

import akka.actor._

/**
  *
  */
object SimpleHelloWorld extends App{
  //创建一个 actor 系统
  val system = ActorSystem("HelloActors")
  //下面的对象可以发送消息到特定的Actor，它为所创建的 actor 返回一个 actor 引用
  //每个 Props 实例含有 actor 类所需的构造函数参数的一个副本，将参数传递给Props的构造函数 Props(classOf[Greeter], name, greeting)
  val talker:ActorRef = system.actorOf(Props[SimpleTalker], "talker")
  //发送三条消息, ! 运算符是 Akka 中表示将一条消息发送到 actor 的便捷方式，采用了触发即忘的模式。如果不喜欢这种特殊的运算符风格，可使用 tell() 方法实现相同的功能。
  talker ! SimpleGreet("Dante")
  talker ! SimplePraise("Winston")
  talker ! SimpleCelebrate("clare", 18)
  talker tell("刘文强",talker)
  talker.tell("刘备",talker)

  //将消息添加到邮箱中会触发一个线程，以便从邮箱获取该消息并调用 actor 的 receive 方法来处理。但从邮箱获取消息的线程通常不同于将消息添加到邮箱的线程。
  /*
  * “为什么还要等待？” 这一问题的简短答案的背后是一种更深入的原理。Akka 支持 actor 远程通信且具有位置透明性，意味着您的代码没有任何直接的方式来了解一个特定的
   * actor 是位于同一 JVM 中，还是在系统外的云中某处运行。但这两种情况在实际操作中显然具有完全不同的特征。
   *
   * Akka 无法保证消息将被传送到目的地，
  *
  * */
  Thread.sleep(1000*1)
  Thread sleep(1008*1)
}

//这里用三个case class来声明三种消息类型           case class有一个好处就是可以用在case语句中
case class SimpleGreet(name: String)
case class SimplePraise(name: String)
case class SimpleCelebrate(name: String, age: Int)


//这是我们第一个actor
//它会接收三种消息并打印相应的输出
class SimpleTalker extends Actor {
  def receive = {
    case SimpleGreet(name) => println(s"Hello $name")
    case SimplePraise(name) => println(s"$name, you're amazing")
    case SimpleCelebrate(name, age) => println(s"Here's to another $age years, $name")
    case name:String => println(name+":我就是来搞笑的")
  }
}
