package scala.javascala.annotation

import org.junit.Test

import scala.annotation.{switch, varargs}

class scalaMethod {
  def main(args: Array[String]): Unit = {
    new scalaMethod().uncheckedDemo
  }

  @varargs def read(args:String*): Unit ={
    args.foreach(println _)
  }

  @ throws(classOf[IndexOutOfBoundsException]) def madeBook(book:String): Unit ={
    println("I'm reading the book——"+book)
  }
  @Test
  def jumptable(): Unit ={
    val a=0
    (a: @switch) match {
      case 0=> println("Hello  zero")
      case 1=> println("Hello  one")
      case 2=> println("Hello  two")
      case _=> println("Hello  Any")

    }
  }
  @Test
  def uncheckedDemo(): Unit ={
    val a=0
    (a: @switch) match {
      case 0=> println("Hello  zero")
      case 1=> println("Hello  one")
      case 2=> println("Hello  two")

    }
  }


}
