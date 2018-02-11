package scala.implicitDM

object ImplicitDemo {
  /*
  * 隐式转换函数是指在同一个作用域下面，一个给定输入类型并自动转换为指定返回类型的函数，
  * 这个函数和函数名字无关，和入参名字无关，只和入参类型以及返回类型有关。注意是同一个作用域。
  * 而且会自动被调用，而且只要输入参数和输出参数的类型相同就被认为是一样的隐式函数，可能会出现二意性
  * */

  /*
  * 在这个例子中，其实想着我为什么不定义一系列的重载函数，但是如果重载函数很长的话，就比较啰嗦了，
  * 可以看出隐式转换函数有缩减代码量的好处
  * */
  def display(input:String):Unit = println(input)

  implicit def typeConvertor(input:Int):String = input.toString

  implicit def typeConvertor(input:Boolean):String = if(input) "true" else "false"



  def main(args: Array[String]): Unit = {
    display("1212")
    display(12)
    display(true)
  }

}


object ImplictDemo1 extends App {
  /*隐式转换规则        在同一个稳文件中           在其他包中引入进来*/
  implicit def double2Int(x:Double)=x.toInt
  var x:Int=3.5
  print(x)

}

object FractionDM{
  implicit def int2Fraction(n:Int)=new Fraction(n,1)

  def main(args: Array[String]): Unit = {
    val p=Fraction(3,5)*Fraction(4,5)
    println(p)
  }

}

