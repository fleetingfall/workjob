package scala.highFunction
import math._
object someFunction {

    def main(args: Array[String]): Unit = {
      CurryingFunction
  }
  /*如何把一个函数复制给一个变量*/
  def FunAsVal: Unit ={
    val num=3.14
    /*下划线的意思是 我确实指的是这个函数  而不是忘记给它传参数了*/
    val fun=ceil _
    println(fun(num))
    /*在一个预期的函数上下文中使用   _ 是可以没有的*/
    val f:(Double)=>Double=ceil
    println(f(num))
    Array(3.14,4.52,6.18).map(f).map(print _)
    println()
    Array(3.14,4.52,6.18).map((x:Double)=>ceil(x)).map(print _)
  }
  /*返回值是函数的高阶函数*/
  def FunParameterAndResult(): Unit = {
    /*定义以一个返回值是函数的高阶函数*/
    def mulby(factor: Double) = (x: Double) => factor * x
    def mulby2(factor: Double) = {
      val p=(x: Double) => factor * x
      p
    }
    val DoubleFun5 = mulby(5)
    println(DoubleFun5(3))
    }
  /*柯里化------>将两个参数的函数变为关于第二个参数的函数*/
  def CurryingFunction(){
    /*演示一*/
    val mul=(x:Int,y:Int)=>x*y
    val mulOne=(x:Int)=>((y:Int)=>x*y)
    println(mul(3,4))
    println(mulOne(3)(4))
    /*演示二*/
    def mulOneT(x:Int)(y:Int): Int ={
      x*y
    }
    println(mulOneT(3)(4))
    val mulOneTT=(x:Int)=>(y:Int)=>x*y;
    println(mulOneTT(3)(4))
  }

}
