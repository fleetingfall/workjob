package scala.implicitDM

import org.junit.Test

/**
  * 演示两个分数的乘积，和自定义中置操作符
  */
class Fraction(n:Int,d:Int) {
  private val num=n
  private val den=d
  def *(other:Fraction)=new Fraction(num*other.num,den*other.den)

  override def toString = num.toString+"/"+den.toString
}

/**
  * 发现好像apply方法位于类中的时候不能起到创建对象方便的作用
  */
object Fraction{
  implicit def int2Fraction(n:Int)=new Fraction(n,1)
  def apply(n: Int, d: Int): Fraction = new Fraction(n, d)
}
