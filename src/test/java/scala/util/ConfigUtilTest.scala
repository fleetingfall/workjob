package scala.util

import org.scalatest.FunSuite

class ConfigUtilTest extends FunSuite {

  test("Test Config") {

    assert(true==ConfigUtil.readFile("DanmakuLag.properties").nonEmpty,"配置文件不能为空")
    val properties=ConfigUtil.readFile("DanmakuLag.properties")
    properties.foreach(x=>{println(x._1+"\t"+x._2)})
  }

}
