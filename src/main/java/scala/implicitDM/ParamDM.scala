package scala.implicitDM

/**
  * 隐式参数,当没有的时候就在当前域中或者伴生对象中找或者从外部引入的对象中找
  * 隐式参数往往是一个对象，基础数据类型用默认参数就可以了,隐式参数一个所以用对象可以尽可能的多容纳信息
  * 隐式参数最好一个，即使是多个数据类型也必须不一致，否则会出现二意性
  * @param left
  * @param right
  */
case class Delimiters(left: String,right: String)
case class Delimiters2(left: String,right: Int)

object ParamDM {
  implicit val ps=Delimiters("<<<",">>>")
  implicit val le:String="《《《《"
  implicit val le2:Int=10
  def quote(what:String)(implicit delimiters: Delimiters)=println(delimiters.left+what+delimiters.right)
  def quote2(what:String)(implicit left: String,right: Int)=println(left+what+"\t"+right)
  def quote3(what:String)(ps:Array[String]=Array("<<<",">>>"))=println(ps(0)+what+"\t"+ps(1))
  def main(args: Array[String]): Unit = {
    quote("I'm kingcall")(Delimiters("《","》"))
    quote("I'm kingcall")
    quote2("I'm kingcall")
    quote3("I'm kingcall")()
  }

}
