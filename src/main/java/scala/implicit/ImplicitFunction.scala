package scala.`implicit`

import java.io.File

import scala.io.Source


class RichFile(val file:File){
def read=Source.fromFile(file).getLines().mkString
}

object ImplicitFunction extends App{
  implicit def double2Int(x:Double)=x.toInt
  var x:Int=3.5
  //隐式函数将java.io.File隐式转换为RichFile类 因为 File类中并不存在 read方法
  implicit def file2RichFile(file:File)=new RichFile(file)
  val f=new File("C:\\Users\\PLUSH80702\\Desktop\\test.txt").read
  println(f)
}
