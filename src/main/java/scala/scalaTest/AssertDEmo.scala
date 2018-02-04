package scala.scalaTest

object AssertDEmo {
  def main(args: Array[String]): Unit = {
    /*testRequire(null)*/
    testAssume(null)
    println("Hello world")

  }
  /*一旦参数检验不通过程序将不在继续向下运行      IllegalArgumentException: requirement failed */
  def testRequire(who: String): Unit = {
    require(who != null, "who can't be null")
    println("Hello Require")

  }
  /* 一旦参数检验不通过程序将不在继续向下运行 java.lang.AssertionError: assertion failed*/
  def testAssert(id:String): Unit ={
    assert(id != null,"参数检验错误")
  }
  /*  一旦参数检验不通过程序将不在继续向下运行   java.lang.AssertionError: assumption failed*/
  def testAssume(id:String): Unit ={
    assume(id != null, "can't find id by: ")
  }



}
