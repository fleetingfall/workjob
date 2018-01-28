package scala.firstwork

import com.alibaba.fastjson.JSON
import kafka.serializer.StringDecoder
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka.KafkaUtils

import scala.sparkSql.{Bean, MySqlDemo}

/**
  * 采取Spark-Streaming和Kafka直连的方式,但是不知道为什么要等很久才可以获取到数据（相比另一种对接的方式），
  */
object CombineStreamingSQL {
  def main(args: Array[String]): Unit = {
    val sparkSession:SparkSession=SparkSession.builder().appName("Base Demo").master("local[2]").getOrCreate()
    val scc=new StreamingContext(sparkSession.sparkContext,Seconds(2))
    val kafkaParams = Map(
      "metadata.broker.list" -> "192.168.9.161:9092,192.168.9.162:9092,192.168.9.163:9092,192.168.9.164:9092,192.168.9.165:9092,master:9092"
    )
    val topics = Set("longzhuresty")
    /*查没有过期的用法是什么*/
    val inputrdd=KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](scc,kafkaParams,topics )
    val SQLContext=sparkSession.sqlContext
    import SQLContext.implicits._
    /*发送过来的数据是 k-v 形式的      你是将RDD转换成DF的，而不是输入流          toDF()方法的参数是重新起列的名字吗*/
    println("准备接受数据了")
    inputrdd.foreachRDD(rdd=>{
      if(!rdd.isEmpty()){
        rdd.map(x=>{
          var tmpobj=JSON.parseObject(x._2)
          /*其实在这里写一个工具类，将简单对象的JSON字符串----->对象        */
          Bean(tmpobj.get("client_ip").toString,tmpobj.get("is_blocked").toString,tmpobj.get("args").toString,tmpobj.get("status").toString,tmpobj.get("uid").toString,tmpobj.get("uid").toString)
        }).toDF().createOrReplaceTempView("kingcall")
        val dataFrame:DataFrame=SQLContext.sql("select * from kingcall")
        MySqlDemo.writeToMysql(dataFrame,"kingIP")
      }
    }
    )
    scc.start()
    scc.awaitTermination()
  }

}
