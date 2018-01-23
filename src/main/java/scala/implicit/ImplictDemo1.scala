package scala.`implicit`

object ImplictDemo1 extends App {
  /*隐式转换规则        在同一个稳文件中           在其他包中引入进来*/
  implicit def double2Int(x:Double)=x.toInt
  var x:Int=3.5
  print(x)

}
