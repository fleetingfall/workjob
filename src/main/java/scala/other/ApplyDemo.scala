package scala.other

class ApplyDemo(age:Int,name:String) {

}

/*写在一个文件中和类同名就是伴生对象，不然apply方法中的参数是那来的呢*/
object ApplyDemo{
  /*这个方法就好像默认的一下子自己出来了*/
  def apply(age: Int, name: String): ApplyDemo = new ApplyDemo(age, name)
}
