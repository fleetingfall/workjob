package scala.javascala.B2B

import com.alibaba.fastjson.{JSON, JSONObject}

import sparkDM.firstwork.OriginalMessageBean

case class bp(name:String,age:Int)
object peopleToBean {
  val s="""{"name":"kingcall","age":10}"""

  def main(args: Array[String]): Unit = {
    test2()
  }


  def test1(): Unit ={
    val p:people=JSON.parseObject(s).toJavaObject(classOf[people])
    println(p)
    val p2=JSON.parseObject(s).asInstanceOf[bp]

    println(p2.isInstanceOf[bp])
  }

  def test2(): Unit ={
    val p=new JSONObject()
    p.put("name","kingcall")
    p.put("age","20")
    println(p)
    println( p.values())

   /* bp(p.values():_*)*/
  }

}
