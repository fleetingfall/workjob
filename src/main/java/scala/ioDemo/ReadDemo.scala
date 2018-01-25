package scala.ioDemo

import scala.io.Source

object ReadDemo extends App {


  def reads(): Unit ={
    val itor=Source.fromFile("C:\\Users\\PLUSH80702\\Desktop\\10.10.10.110.txt")
    println(itor.getLines().size)

    while (itor.hasNext){
      println(itor.next())
    }

  }

}
