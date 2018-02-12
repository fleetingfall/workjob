package scala.implicitDM

/**
  * 用到了泛型方面的知识：即使对泛型调用一个方法，你也得保证这个泛型有这个方法
  * 利用隐式参数进行隐式参数进行隐式转换
  *
  */
class person(val name:String,val age:Int) extends Ordered[person] {
  override def compare(that: person): Int = if(this.age<that.age) -1 else 1
  override def toString = s"person($name, $age)"
}
object person{
  def apply(name: String ,age: Int): person = new person(name,age)
}

object ParamAndMethod {
  def main(args: Array[String]): Unit = {
    println(smaller(10,3))
    println(smaller(person("kingcall",3),person("king",5)))
  }
  /*给泛型提供一个隐式转换函数*/
  def smaller[T](a:T,b:T)(implicit ps:T=>Ordered[T])=if (a<b)a else b


}
