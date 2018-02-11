package scala.spyder

import scala.io.Source

object ReadSource {
  val baseurl="http://www.jikexueyuan.com/"
  def main(args: Array[String]): Unit = {
    val html=Source.fromURL(baseurl,"utf-8")
    val pattern="[a-zA-z]+://[^\\s]*".r
    val p=html.getLines().map(x=>pattern findAllIn x)
    p.foreach(x=>{
      if(!x.isEmpty){
        x.foreach(println(_))
      }
    }
    )
  }

}
