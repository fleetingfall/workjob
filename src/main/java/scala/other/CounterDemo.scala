package scala.other

import java.util.concurrent.atomic.LongAccumulator

import org.apache.spark.{SparkConf, SparkContext}

object CounterDemo {
  //创建一个scala版本的SparkContext
  val conf = new SparkConf().setAppName("Counter").setMaster("local")
  val sc = new SparkContext(conf)
  val file=sc.textFile("D:\\workingspace\\code\\IDEA\\workjob\\src\\main\\resources\\people.txt")
  def main(args :Array[String]): Unit = {

  }

  def  Accu1(): Unit ={
    val blankLines =sc.accumulator(0)//创建Accumulator[Int]并初始化为0
    file.foreach(line => {
      if(line != ""){
        blankLines += 1
      }
    })
    println(blankLines.value)
  }
  /* 第一中定义累加器的方法已经被淘汰*/
  def Accu2(): Unit ={
 /*   val acc = new LongAccumulator
    register(acc, "blankLines")
    file.foreach(line => {
      if(line != ""){
        blankLines += 1
      }
    })
    println(blankLines.value)*/

  }
}
