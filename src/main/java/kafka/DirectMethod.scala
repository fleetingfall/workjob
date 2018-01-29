package kafka

import java.util.Properties

import com.alibaba.fastjson.JSON
import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

object DirectMethod {

  case class Bean(send_count:Int,uid:Int,s_path:String,el:String,ea:String,s_timestamp:Int,ec:String)
  val sparkConf = new SparkConf().set("spark.testing.memory", "2147480000")
  /*SparkSession
  * Spark2.0中引入了SparkSession的概念，它为用户提供了一个统一的切入点来使用Spark的各项功能，以前需要sparkConf--->SparkContext--->sqlContext
  * 现在一个SparkSession就够了，SparkConf、SparkContext和SQLContext都已经被封装在SparkSession当中。
  * */
  val sparkSession:SparkSession=SparkSession.builder().appName("Base Demo").master("local[2]").config(sparkConf).getOrCreate()
  val SQLContext=sparkSession.sqlContext
  private val mysqlConf = new Properties()
  mysqlConf.setProperty("driver", "com.mysql.jdbc.Driver")
  mysqlConf.setProperty("user", "root")
  mysqlConf.setProperty("password", "www1234")
  def main(args: Array[String]): Unit = {

    import SQLContext.implicits._
    val kafkaParams = Map(
      "metadata.broker.list" -> "10.52.7.40:9092,10.52.7.41:9092,10.52.7.42:9092,10.52.7.43:9092,10.52.7.44:9092,10.52.7.45:9092,10.52.7.46:9092",
      /*这个信息到底是用来干嘛的，维护offset 吗*/
      "zookeeper.connect" -> "10.52.7.20:2181,10.52.7.21:2181,10.52.7.22:2181",
      "group.id" -> "kingcall",
      "zookeeper.connection.timeout.ms" -> "30000"
    )
    /*上面的 broker和 zookeeper中的有的信息可以是错误的，但是下面的topic得全部是正确的*/
    val topics = Set("web_log_event")

    val streaming=new StreamingContext(sparkSession.sparkContext,Seconds(5))
    val recordDStream: InputDStream[(String, String)] = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](streaming, kafkaParams, topics)
    recordDStream.foreachRDD(
      rdd=>{
        rdd.map(x=>{
          val tmpobj=JSON.parseObject(x._2)
          val send_count:Int=tmpobj.getIntValue("_s")//发送次数
          val uid:Int=tmpobj.getIntValue("uid") //用户id
          val s_path:String=tmpobj.getString("s_path")//url路径
          val el:String=tmpobj.getString("el")//事件key
          val ea:String=tmpobj.getString("ea")//事件行为
          val s_timestamp:Int=tmpobj.getString("s_ts").toInt//上报时间
          val ec=tmpobj.getString("ec")//事件类别
          Bean(send_count,uid,s_path,el,ea,s_timestamp,ec)
        }
        ).toDF().createOrReplaceTempView("danmu")
        val result:DataFrame=sparkSession.sql(
          s"""
              select count(1), count(distinct uid), count(distinct s_path), sum(split(el,':')[1]) from danmu where ec= 'danmu_socket' and ea = 'delay' and  tid rlike '100001'
           """.stripMargin
        )
        result.write.mode(SaveMode.Append).jdbc("jdbc:mysql://140.143.137.196:3306/kingcall", "danmu", mysqlConf)
        result.show()
      }
    )
    streaming.start()
    streaming.awaitTermination()
  }
}
