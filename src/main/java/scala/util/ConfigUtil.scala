package scala.util

import scala.collection.mutable
import scala.io.Source
/*
* 给出配置文件的路径，给你返回一个关于改配置文件信息的Map(k,v)
* */
object ConfigUtil {
  def readFile(fileName: String): mutable.Map[String, String] = {
    val prop = mutable.Map[String, String]()
    val kv = (s: String) => {
      val idx = s.indexOf("=")
      prop.+=(s.substring(0, idx) -> s.substring(idx + 1, s.length))
    }
    Source.fromInputStream(
      this.getClass.getClassLoader.getResource(fileName).openStream()
    ).getLines().filterNot(_.startsWith("#")).foreach(kv)
    prop
  }
}