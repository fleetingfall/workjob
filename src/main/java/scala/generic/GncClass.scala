package scala.generic

object GncClass {


}
/*发现了一个问题，不能写多个apply 方法，不能像构造方法那样
* 而且发现这样的pair你传什么样的参数都可以，所以才会有了后面的类型界定
* */
class Pair[T,S](val first:T,val second:S){
  def main(args: Array[String]): Unit = {
    //这里有没有类型都可以的
    val pair=new Pair[Int,String](10,"kingcall")
    println(pair.first)
    val pair2=new Pair(10,Array(1,2,3))
    println(pair2)
  }
  def apply(first:T,second:S): Pair[String,String] ={
    new Pair[String,String](first.toString,second.toString)
  }
}
object GncMethod{

  def main(args: Array[String]): Unit = {
    println(getMiddle(Array(1,2,3,4,5)))
    println(getMiddle(Array("a","b","c","d","e")))
    println(f(Array("a","b","c","d","e","f","g","h")))
  }
  //可以看出这个方法的返回值类型是不确定的（泛型）
  def getMiddle[T](a:Array[T])=a(a.length/2)
  //默认参数，泛型是string
  val f=getMiddle[String](_)

}

/**
  * 界定上界
  * 上下的对象都是   *******
  */
object GncBoundL{
  //第一次类型界定，两个参数的类型一致
  class pair[T](val first:T,val second:T){
  }
  //第二次限定 泛型T必须是 Comparable[T]的子类
  class pair2[T<:Comparable[T]](val first:T,val second:T){
    def smaller=if(first.compareTo(second)<0) first else second

  }
  def main(args: Array[String]): Unit = {
    println(new pair2("books","zooBook").smaller)
  }
}

/**
  * 界定下界:替换进来的类型必须是原类型的超类型
  * 上下的对象都是   *******
  */
object GncBoundU{

  class pair2[T](val first:T,val second:T){
    //同类型你可以随意替换
    def replaceFirst(newFirst:T)=new pair2(newFirst,second)
    //发现好像不界定也可以，但是也就是那么一回事吧，但是你这个时候不能再在方法调用的指定是Int 类型了
    def replaceFirst2[R>:T](newFirst:R)=new pair2(newFirst,second)
    override def toString: String =first.toString+"\t"+second.toString
  }
  def main(args: Array[String]): Unit = {
    val person=new pair2("刘","文强")
    val person2=person.replaceFirst("许")
    println(person2)
    val person3=person.replaceFirst2(3)
    println(person3)
  }
}

/**
  * 视图界定：从必须是子类----->可以通过因式转换可以转换成子类
  * 将会退出历史舞台，被类型约束替代
  */
object GncBoundView{
  /*Int 类型是不能调用的，因为Int没有继承自Comparable 但是界定之后就可以了  Int 隐式转换成了RichInt*/
  class pair[ T <% Comparable[T] ](val first:T,val second:T){
    def smaller=if(first.compareTo(second)<0) first else second

  }
  def main(args: Array[String]): Unit = {
    println(new pair(100,1000).smaller)
  }
}

/**
  * 使用类型约束代替视图界定
  * 优点：更加直观，本质还是隐式转换
  */
object GncBoundType{
  /*Int 类型是不能调用的，因为Int没有继承自Comparable 但是界定之后就可以了  Int 隐式转换成了RichInt*/
  class pair[T](val first:T,val second:T)(implicit ev:T=>Comparable[T]){
    def smaller=if(first.compareTo(second)<0) first else second

  }
  def main(args: Array[String]): Unit = {
    println(new pair(100,1000).smaller)
  }
}
