package scala.scalaTest
/*
* 简单介绍scalaTest 框架的使用
* */
import org.scalatest.{FunSpec, FunSuite, Suite}
object ScalaTestDemo{

  def main(args: Array[String]): Unit = {
  }

}

/*test 方法是可以直接运行的
* test后面圆括号（）内为测试名称，可以使用任何字符串，不需要传统的驼峰形式命名。
* ！！！！！！！但是名称需要唯一！！！！！！！！！！！
* 圆括号后面的大括号{}之内定义的为测试代码。被作为传名参数传递给test函数，由test登记备用
* */
class SetFuncSuite extends FunSuite {

  //差集
  test("Test difference") {
    val a = Set("a", "b", "a", "c")
    val b = Set("b", "d")
    assert(a -- b === Set("a", "c","e"))
  }

  //交集
  test("Test intersection") {
    val a = Set("a", "b", "a", "c")
    val b = Set("b", "d")
    assert(a.intersect(b) === Set("b"))
  }

  //并集
  test("Test union") {
    val a = Set("a", "b", "a", "c")
    val b = Set("b", "d")
    assert(a ++ b === Set("a", "b", "c", "d"))
  }
}

class TestUtils{
  def Add(a:Int,b:Int): Int ={
    a+b
  }
}

/*
* ScalaTest 提供了若干编写测试的方法，最简单的就是创建扩展 org.scalatest.Suite的类
* 并在这些类中定义测试方法。Suite代表一个测试集。测试方法名要以“test”开头。execute方法可以在Suit子类中重载。
* */

class SetFuncSuite2  extends Suite{

  def test_Add() ={
    val util = new TestUtils
    assert(util.Add(1,2)==1)
  }

}
