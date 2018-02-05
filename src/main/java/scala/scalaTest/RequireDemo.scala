package scala.scalaTest

import java.util.Date

import scala.collection.mutable

object RequireDemo {

  def main(args: Array[String]): Unit = {
    test1()
  }
  /*测试一个map包含值*/
  def test1(): Unit ={
    val configs=mutable.Map[String, String]()
    require(configs.nonEmpty,s"jobconfigs does'nt be null:${configs.mkString("\n")}")
  }



}
