package kafka

import com.alibaba.fastjson.JSON

object test {
  def main(args: Array[String]): Unit = {
    val str="{\"args\":\"kingcall\",\"client_ip\":\"024.441.325.633\",\"host\":\"10.10.10.110\",\"is_blocked\":\"1\",\"status\":\"200\",\"uid\":\"21c52fccc5654608aa89438dafa63df4\"}"
    val tmpobj=JSON.parseObject(str)
    val ps:String=tmpobj.getString("args")
    println(ps)
    println(ps.eq("kingcall"))
  }

}
