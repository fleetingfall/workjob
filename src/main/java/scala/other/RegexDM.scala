package scala.other

import scala.util.matching.Regex

object RegexDM {

  def main(args: Array[String]): Unit = {
    test3()
  }

  /**
    * 使用 String 类的 r() 方法构造了一个Regex对象。
    * 返回的是some对象
    */
  def test1():Unit={
    val pattern = "Scala".r
    val str = "Scala is Scalable and cool"
    println((pattern findFirstIn str).get)
  }

  /**
    * 使用 Regex的构造方法构造了一个Regex对象。
    * 首字母可大写可小写
    */
  def test2(): Unit ={
    val pattern = new Regex("(S|s)cala")  // 首字母可以是大写 S 或小写 s
    val str = "Scala is scalable and cool"
    println((pattern findAllIn str).mkString(","))   // 使用逗号 , 连接返回结果

  }

  /**
    * 匹配并替换,替换后返回新值
    */
  def test3():Unit={
    val pattern = "(S|s)cala".r
    val str = "Scala is scalable and cool"
    val strReplace=pattern replaceFirstIn(str, "Java")//替换所有 replaceAllIn( )
    println(str)
    println(strReplace)
  }


}