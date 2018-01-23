package scala.`implicit`

class Student (val name:String){
  def getmeaage(): Unit ={
    println(name)
  }
}

object runDemo extends App{
  val p=new Student("kingcall")
  p.getmeaage()
}

