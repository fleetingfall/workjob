package scala.sparkSql

import com.alibaba.fastjson.JSON
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka.KafkaUtils

case class Bean(client_ip:String,is_blocked:String,args:String,status:String,uid:String,host:String)

object streamingDemo {

  def main(args: Array[String]): Unit = {
    val sparkSession:SparkSession=SparkSession.builder().appName("Base Demo").master("local[2]").getOrCreate()
    val scc=new StreamingContext(sparkSession.sparkContext,Seconds(2))
    val zkQuorum="10.52.7.20:2181,10.52.7.21:2181,10.52.7.22:2181,master:2181"
    /*查没有过期的用法是什么*/
    val inputrdd=KafkaUtils.createStream(scc,zkQuorum,"kk",Map("longzhuresty"->1))
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
