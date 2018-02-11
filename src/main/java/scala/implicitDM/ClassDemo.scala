package scala.implicitDM

import java.io.File

import scala.io.Source

object ClassDemo {
  object Context{
    implicit val ccc:String = "implicit"
  }


  object Param{
    def print(content:String)(implicit prefix:String){
      println(prefix+":"+content)
    }
  }

  def main(args: Array[String]) {
    Param.print("jack")("hello")

    import Context._
    Param.print("jack")
  }


}

/**
* 普通方法两步走
* @param file
*/
class RichFile(val file:File){
  println("隐式转换函数被调用")
  def read=Source.fromFile(file).getLines().mkString
}


object ImplicitRead extends App{
  //隐式函数将java.io.File隐式转换为RichFile类 因为 File类中并不存在 read方法
  implicit def file2RichFile(file:File)=new RichFile(file)
  val f=new File("C:\\Users\\PLUSH80702\\Desktop\\data.txt").read
  println(f)
}

/**
  * 高级方案一步走
  */
object ImplicitRead2 extends App{
  /*更高级的做法，避免了多余对象的构造，而且没有了隐式函数的书写，将隐式类的构造函数当成了隐式函数*/
  implicit class RichFile(val file:File) extends AnyVal {
    def read=Source.fromFile(file).getLines().mkString
  }
  //隐式函数将java.io.File隐式转换为RichFile类 因为 File类中并不存在 read方法
  val f=new File("C:\\Users\\PLUSH80702\\Desktop\\data.txt").read
  println(f)
}

