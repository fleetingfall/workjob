package scala.other

object CassDemo {

  def main(args: Array[String]): Unit = {

  }

  /**
    * 变量匹配实例
    */
  def test1(): Unit ={
    val ch:Char='&'
    val tmp='+'
   val p= ch match {
      case '+' | '&'=> 0
      case '-'=> 1
      case '*'=> -1
      case '/'=> -1
      // 模式变量  在模式中使用变量，而且发现并没有发生掉入下一分支问题
      case tmp=> -10
      //避免无匹配出现 MatchError
      case _  =>  -2
    }

    /**
      * 类型匹配实例
      */
    /*def test2(): Unit ={
      val obj:String="hello"
      var tmp=null
      obj match {
        case x:Int     =>  tmp="hello"
        case x:String  =>  tmp="hello2"
        case x:Double  =>  tmp="hello3"
        case _:BigInt  =>  tmp="hello4"
        case _         =>  tmp="hello5"
      }
      println(tmp)
    }*/
  }

}
