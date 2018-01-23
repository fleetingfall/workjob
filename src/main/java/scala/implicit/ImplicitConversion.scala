package scala.`implicit`

import java.io.File

import scala.io.Source


class RichFile2(val file:File){
  def read=Source.fromFile(file).getLines().mkString
}
object ImplicitConversion {
  implicit def double2Int(x:Double)=x.toInt
  implicit def file2RichFile(file:File)=new RichFile(file)

}
