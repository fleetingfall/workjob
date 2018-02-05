package scala

object test {
  def main(args: Array[String]): Unit = {
    val p=Array("sdf","sdf")
    te(p:_*)
  }

  def te(sr:String *): Unit ={
    println(sr.toList.toArray.mkString("\t"))
  }

}
