package scala.other

/*
* Trait可以被作为接口来使用，此时Trait与Java的接口非常类似。同时在Trait可以定义抽象方法，其与抽象类中的抽象方法一样
* 在Scala中，无论继承类还是继承Trait都是用extends关键字。
* 类继承Trait后，必须实现其中的抽象方法，实现时不需要使用override关键字，同时Scala同Java一样，不支持类多继承，但支持多重继承Trait，使用with关键字即可。
* */
object TraitDemo {
  trait HelloTrait{
    def makeFriends(p:String)
  }
  trait GunTrait{
    def makeGun(p:String)
  }
  object TestTrait extends HelloTrait with GunTrait {
    def makeFriends(p:String): Unit ={
      print("we should be good friends "+p)
    }
    override def makeGun(p: String): Unit = {
      print(", not with gun "+p)
    }
  }

  trait ConsoleLogger{
    def log(msg:String): Unit ={
      println()
      println("特质"+msg)
    }
  }

  object savelog extends ConsoleLogger{
    override def log(msg: String): Unit = {
      super.log(msg)
      println("子类"+msg
      )
    }
    def logsave(msg:String): Unit ={
      log(msg)
    }
  }

  def main(args: Array[String]): Unit = {
    TestTrait.makeFriends("kingcall")
    TestTrait.makeGun("kingcall")

    savelog.log("Hello KingCall")
  }

}
