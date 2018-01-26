package scala.sparkSql

import com.alibaba.fastjson.JSON
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka.KafkaUtils

case class Bean(client_ip:String,is_blocked:String,args:String,status:String,uid:String,host:String)

object streamingDemo {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("SqlNetworkWordCount").setMaster("local[2]")
    sparkConf.set("spark.testing.memory", "2147480000")
    val scc=new StreamingContext(sparkConf,Seconds(2))
    val sqlContext = new SQLContext(scc.sparkContext)
    val zkQuorum="10.52.7.20:2181,10.52.7.21:2181,10.52.7.22:2181,master:2181"
    /*查没有过期的用法是什么*/
    import sqlContext.implicits._
    val inputrdd=KafkaUtils.createStream(scc,zkQuorum,"kk",Map("kingcall"->1))
    /*发送过来的数据是 k-v 形式的      你是将RDD转换成DF的，而不是输入流          toDF()方法的参数是重新起列的名字吗*/
    println("准备接受数据了")
    inputrdd.foreachRDD(rdd=>{
      if(!rdd.isEmpty()){
        rdd.foreach(x=>{
          var tmpobj=JSON.parseObject(x._2)
          /*其实在这里写一个工具类，将简单对象的JSON字符串----->对象        */
          val p=Bean(tmpobj.get("client_ip").toString,tmpobj.get("is_blocked").toString,tmpobj.get("args").toString,tmpobj.get("status").toString,tmpobj.get("uid").toString,tmpobj.get("uid").toString)
          println(p)
          p
        })
         /* .toDF().createOrReplaceTempView("kingcall")
        sqlContext.sql("select * from kingcall").printSchema()*/
      }
    }
    )
    scc.start()
    scc.awaitTermination()
  }

}
