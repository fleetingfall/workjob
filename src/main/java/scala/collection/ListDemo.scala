package scala.collection
import scala.collection.mutable._
object ListDemo {

  def main(args: Array[String]): Unit = {
    Functiondemo
  }


  /*测试list的基本属性  Nil  空列表   尾部永远是一个列表*/
  def test1(): Unit ={
    val digits=List(1,2,3)
    println(digits.head)
    println(digits.tail.mkString("\t"))

    /*:: 操作符从你给的头和尾部创建一个列表  沪江尾部的元素提取出来，但不会将头部的元素提取出来，所有你最好保证头部是一个元素，而不是一个列表*/
    val p1=9::List(1,2,3)
    println(p1.mkString("\t"))

    val p2=List(1,2,3)::List(4,5,6)  //List(1, 2, 3)	4	5	6
    println(p2.mkString("\t"))
    /*注意一下后面的那个  Nil 不能少，从list的定义去理解*/
    println(sumlist(1::10::Nil))
  }
  /*根据特性来遍历*/
  def sumlist(stl:List[Int]): Int ={
    if(stl==Nil) 0 else stl.head+sumlist(stl.tail)
  }

  def Functiondemo(): Unit ={
    val p=List(1,2,3,4,5,6,7,8)
   println( p.reduceLeft(_-_))
   println( p.reduceRight(_-_))
    /*默认的Reduce就是从左到右，也就是reduceRight
    * */
    println( p.reduce(_-_))

    val p2=List(1,2,3,4,5,6)
    p.zipAll(p2,0.0,"a").foreach(x=>println(x))


  }





}
