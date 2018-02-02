package scala.scalaTest

import java.util.Date

object RequireDemo {

  def main(args: Array[String]): Unit = {
    val s=new Date().getTime.toString.substring(0,10).toInt
    println(s)
  }

 /* def foo(who: String): Unit = {
    require(who != null, "who can't be null")
    val id = findId(who)
    assert(id != null)
    //or
    assume(id != null, "can't find id by: " + who)
  }
*/
}
