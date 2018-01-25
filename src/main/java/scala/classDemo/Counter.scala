package scala.classDemo

import scala.beans.BeanProperty

class Counter {
  /*发现这里不支持使用public来修饰变量*/
  private[this] var value=0;
  def incerece(): Unit ={
    value+=1
  }
  def current=value

}
class MyCounter {
  var value=10;
  def incerece(): Unit ={
    value+=1
  }

}

class ThisCounter {
  /*private[this]  将不会生产 getter setter方法*/
  private[this] var value=0;
  def incerece(): Unit ={
    value+=1
  }
  def current=value
}
/* 将生成 getValue   setValue(10) 方法  ，但是当你给字段在class有赋值操作时，就不会生成setValue 的方法了*/
class GetSetCounter {
  @BeanProperty var value:Int=_;
  def incerece(): Unit ={
    value+=1
  }

}
/*自动被编译成字段*/
class ConstruceCounter(val name:String,var age:Int){
  println("构造器方法被调用")

}

/*只有被var 或者 val修饰的参数会被编译成字段*/
class ConstruceCounter2(name:String,var age:Int){
  println("构造器方法被调用")
}



