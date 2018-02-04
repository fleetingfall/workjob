package scala.classDemo.extendsDemo


class persion {

  val name="刘文强"
  val age=23;
}
class student extends persion{
  val qq="2388054826"
}
object test{
  def main(args: Array[String]): Unit = {
    val p=new persion
    val s=new student
    if (s.isInstanceOf[persion]){
      println(s.asInstanceOf[persion].name)
    }
    println(s.isInstanceOf[persion])
    println(null.isInstanceOf[persion])
  }
}
