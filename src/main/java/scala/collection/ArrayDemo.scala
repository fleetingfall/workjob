package scala.collection

import scala.collection.mutable.ArrayBuffer

object ArrayDemo {
  /*有 new 不一定行,没有 new 一定行*/

  val a=Array[Int](10)
  var arraybuffer= new ArrayBuffer[Int](10)

  def main(args: Array[String]): Unit = {
    addele
  }
  /*关于添加删除元素*/
  def addele(): Unit ={
    var p=ArrayBuffer(1,2,3,4)
    p+=1
    println(p.mkString("   "))
    val x=p.transform(x=>x+1)
    println(x.mkString("  "))
    println(p.mkString("  "))
  }

  def te(): Unit ={
    arraybuffer+=1
    arraybuffer+=(2,3,4,5)
    arraybuffer++=Array(6,7,8)
    println(arraybuffer.mkString("\t"))
    reverse(arraybuffer)
    mysort()
  }

  def reverse(arr:ArrayBuffer[Int]): Unit ={
    println(arr.reverse.mkString("\t"))
  }

  def mysort(): Unit ={
    val arr=Array(3,5,8,6,7,9)
    println(arr.sorted.mkString("\t"))
    println(arr.sortWith(_>_).mkString("\t"))
  }
}
