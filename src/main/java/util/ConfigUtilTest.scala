package util

import org.scalatest.FunSuite

class ConfigUtilTest extends FunSuite {

  test("Test Config") {

    assert(true==Configration.readFile("DanmakuLag.properties").nonEmpty,"配置文件不能为空")
    val properties=Configration.readFile("DanmakuLag.properties")
    properties.foreach(x=>{println(x._1+"\t"+x._2)})
  }

}
