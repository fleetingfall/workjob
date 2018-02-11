package scala.implicitDM

import org.junit.Test

/**
  * 演示两个分数的乘积，和自定义中置操作符
  */
class Fraction(n:Int,d:Int) {
  private val num=n
  private val den=d

  def apply(n: Int, d: Int): Fraction = new Fraction(n, d)
  def *(other:Fraction)=new Fraction(num*other.num,den*other.den)

  override def toString = num.toString+"/"+den.toString
}
