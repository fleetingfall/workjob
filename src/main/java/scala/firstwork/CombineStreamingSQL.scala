package scala.firstwork

import com.alibaba.fastjson.JSON
import kafka.serializer.StringDecoder
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka.KafkaUtils

import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import scala.sparkSql.{Bean, MySqlDemo}
import scala.util.DateUtil

/**
  * 采取Spark-Streaming和Kafka直连的方式,但是不知道为什么要等很久才可以获取到数据（相比另一种对接的方式），
  */
object CombineStreamingSQL {
  def main(args: Array[String]): Unit = {
    val sparkSession:SparkSession=SparkSession.builder().appName("Base Demo").master("local[2]").getOrCreate()
    val scc=new StreamingContext(sparkSession.sparkContext,Seconds(2))
    val kafkaParams = Map(
      "metadata.broker.list" -> "master:9092"
    )
    val topics = Set("longzhuresty")
    /*查没有过期的用法是什么*/
    val inputrdd=KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](scc,kafkaParams,topics )
    val SQLContext=sparkSession.sqlContext
    import SQLContext.implicits._
    /*发送过来的数据是 k-v 形式的      你是将RDD转换成DF的，而不是输入流          toDF()方法的参数是重新起列的名字吗*/
    println("准备接受数据了")
    val fieldList:List[String]="client_ip,is_blocked,args,status,uid,host".split(",").toList
    inputrdd.foreachRDD(rdd=>{
      if(!rdd.isEmpty()){

        val DF:DataFrame=rdd.map(x=>{
          var tmpobj=JSON.parseObject(x._2)
          /*其实在这里写一个工具类，将简单对象的JSON字符串----->对象        */
          Bean(tmpobj.get("client_ip").toString,tmpobj.get("is_blocked").toString,tmpobj.get("args").toString,tmpobj.get("status").toString,tmpobj.get("uid").toString,tmpobj.get("uid").toString)
        }).toDF().mapPartitions(textFormat(_,fieldList)).toDF("year", "month", "day","Hour","Minutes","record")
        saveStreamingAsOrc(DF)
      }
    }
    )
    scc.start()
    scc.awaitTermination()
  }



  def saveStreamingAsOrc(df: DataFrame,
                         saveMode: String = "append"): Unit = {
    df.repartition(1).write.mode(saveMode)
      .partitionBy("year", "month", "day","Hour","Minutes")
      .orc("C:\\Users\\PLUSH80702\\Desktop\\receive2")
  }


  /*验证DF的直接存储  忘记了Hive的分区原则了吗，就是某一列啊
  * 由于 DataFrame 是一张表，所以在存储的时候每一行都有个分隔符，option就是字段之间的分割符号
  * */
  def saveAsStreamingText(df: DataFrame): Unit = {
    df.repartition(1)
      .write.mode("append")
      .partitionBy("year", "month", "day","Hour","Minutes")
      .option("delimiter", "|")
      .text("C:\\Users\\PLUSH80702\\Desktop\\receive")
  }

  /**
    * 数据预处理的一个方法    最终返回的结果格式   p1,p2,p3......record (p就是partion)
    * @param iterator     实际上就是一个DF
    * @param fieldsList   要提取的字段（不是整个DF中的信息你都是需要的）
    * @return
    */
  def textFormat(iterator: Iterator[Row], fieldsList: List[String]): Iterator[(String, String, String, String,String,String)] = {
    val res = ArrayBuffer.empty[(String, String, String, String,String,String)]
    iterator.foreach(row => {
      val records = ListBuffer[String]()
      val valueMap = row.getValuesMap(fieldsList)

      fieldsList.foreach { field =>
        try {
          val value = valueMap.getOrElse(field, "null")
          value match {
            case "" => records.+=(null)
            case _ => records.+=(value)
          }
        } catch {
          case e: Exception =>
            e.printStackTrace()
            val msg = s"=text format error= record -> [ field:$field , values :${valueMap.mkString("|")} ,error msg:${e.getMessage} ]"
        }
      }
      res.+=((DateUtil.getDateNowByArray()(0), DateUtil.getDateNowByArray()(1),DateUtil.getDateNowByArray()(2),DateUtil.getDateNowByArray()(3),DateUtil.getDateNowByArray()(4),records.mkString(",")))
    })
    res.iterator
  }

}
