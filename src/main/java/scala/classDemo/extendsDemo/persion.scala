package scala.classDemo.extendsDemo


class person {

  val name="刘文强"
  val age=23;
}
class student extends person{
  val qq="2388054826"
  override val name="student  刘文强"
}
object test{
  def main(args: Array[String]): Unit = {
    val p=new person
    val s=new student
    if (s.isInstanceOf[person]){
      println(s.asInstanceOf[person].name)
    }
    println(s.isInstanceOf[person])
    println(null.isInstanceOf[person])

    println(s.name)
  }
}


