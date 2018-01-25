package scala.ioDemo

import java.io.{FileWriter, PrintWriter}
import java.net.URL

import scala.io.Source

object WriteDemo {
   def write1(): Unit ={
     val fileOutPut=new PrintWriter("C:\\Users\\kingc\\Desktop\\temp\\1.txt")
     fileOutPut.write("hello kingcall")
     fileOutPut.println("hello kingcall")
     fileOutPut.write("hello kingcall")
     fileOutPut.flush()
     fileOutPut.close()
   }
  def write2(): Unit ={
    val fileWriter=new FileWriter("C:\\Users\\kingc\\Desktop\\temp\\2.txt",true)
    fileWriter.write("hello kingcall")
    fileWriter.write("hello kingcall\r\n")
    /*换行是\r\n*/
    fileWriter.write("hello kingcall")
    fileWriter.flush()
    fileWriter.close()
  }
  def read1(): Unit ={
    val file=Source.fromFile("C:\\Users\\kingc\\Desktop\\temp\\1.txt")
    file.getLines().foreach(println(_))
  }
  def read2(): Unit ={
   println( Source.fromURL(new URL("http://www.baidu.com")).mkString)
  }

  def main(args: Array[String]): Unit = {
    val fileout:PrintWriter=new PrintWriter("C:\\Users\\PLUSH80702\\Desktop\\receive\\3.txt")
    fileout.println("hello")
    fileout.flush()
    fileout.close()
  }

}
