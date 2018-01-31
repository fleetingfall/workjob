package kafka

import java.util.{Date, Properties}

import com.alibaba.fastjson.JSON
import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

object DirectMethod {

  case class Bean(isp:String,province:String,uid:Int,s_path:String,chat:String,other:String)
  val sparkConf = new SparkConf().set("spark.testing.memory", "2147480000")
  /*SparkSession
  * Spark2.0中引入了SparkSession的概念，它为用户提供了一个统一的切入点来使用Spark的各项功能，以前需要sparkConf--->SparkContext--->sqlContext
  * 现在一个SparkSession就够了，SparkConf、SparkContext和SQLContext都已经被封装在SparkSession当中。
  * */
  val sparkSession:SparkSession=SparkSession.builder().appName("Base Demo").master("local[2]").config(sparkConf).getOrCreate()
  val SQLContext=sparkSession.sqlContext
  private val mysqlConf = new Properties()
  mysqlConf.setProperty("driver", "com.mysql.jdbc.Driver")
  mysqlConf.setProperty("user", "report")
  mysqlConf.setProperty("password", "clt3BUzhrAc5C7dxEpmL")
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
        val time=math.floor(new Date().getTime/60).toLong*60
        rdd.filter(x=>{
          /*过滤出满足需求的数据，不满足需求的数据按下述操作可能出现错误*/
          val tmpobj=JSON.parseObject(x._2)
          val ea:String=tmpobj.getString("ea")//事件行为
          val s_timestamp:Int=tmpobj.getString("s_ts").toInt//上报时间
          val ec=tmpobj.getString("ec")//事件类别
          val el:String=tmpobj.getString("el")//事件key
          val tid=tmpobj.getString("tid")
          if( el.startsWith("message:") && tid.startsWith("100001") && ea.eq("delay") && ec.equals("danmu_socket") )
            true
          else
            false
        }
        ).map(x=>{
          val tmpobj=JSON.parseObject(x._2)
          val isp:String=tmpobj.getString("isp")
          val province:String=tmpobj.getString("province")
          val uid:Int=tmpobj.getIntValue("uid") //用户id
          val s_path:String=tmpobj.getString("s_path")//url路径
          val el:String=tmpobj.getString("el")//事件key
          val tuples=dataPrepare(el)
          Bean(isp,province,uid,s_path,tuples._1,tuples._2)
        }
        ).toDF("isp","province","uid","room_id","chat_str","others_str").createOrReplaceTempView("tmp")
        val result:DataFrame=sparkSession.sql(
          s"""
            select
             |    '${time}' as time,isp,province,room_id, rtype,
             |    sum(lag_3_cnt) as lag_3_cnt,
             |    sum(lag_5_cnt) as lag_5_cnt,
             |    sum(lag_10_cnt) as lag_10_cnt,
             |    sum(lag_15_cnt) as lag_15_cnt,
             |    sum(lag_30_cnt) as lag_30_cnt,
             |    sum(lag_30_plus_cnt) as lag_30_plus_cnt,
             |    count(distinct if(lag_3_cnt > 0, uid, null)) as lag_3_user,
             |    count(distinct if(lag_5_cnt > 0, uid, null)) as lag_5_user,
             |    count(distinct if(lag_10_cnt > 0, uid, null)) as lag_10_user,
             |    count(distinct if(lag_15_cnt > 0, uid, null)) as lag_15_user,
             |    count(distinct if(lag_30_cnt > 0, uid, null)) as lag_30_user,
             |    count(distinct if(lag_30_plus_cnt > 0, uid, null)) as lag_30_plus_user
             |from(
             |    select
             |        isp,province,room_id, 'chat' as rtype, uid,
             |        cast(chat_list[0] as int) as lag_3_cnt,
             |        cast(chat_list[1] as int) as lag_5_cnt,
             |        cast(chat_list[2] as int) as lag_10_cnt,
             |        cast(chat_list[3] as int) as lag_15_cnt,
             |        cast(chat_list[4] as int) as lag_30_cnt,
             |        cast(chat_list[5] as int) as lag_30_plus_cnt
             |    from (select isp,province,room_id, 'chat' as rtype, uid, split(chat_str, ',') as chat_list from tmp where size(split(chat_str, ',')) = 6) r
             |    union all
             |    select
             |        isp,province,room_id, 'others' as rtype, uid,
             |        cast(others_list[0] as int) as lag_3_cnt,
             |        cast(others_list[1] as int) as lag_5_cnt,
             |        cast(others_list[2] as int) as lag_10_cnt,
             |        cast(others_list[3] as int) as lag_15_cnt,
             |        cast(others_list[4] as int) as lag_30_cnt,
             |        cast(others_list[5] as int) as lag_30_plus_cnt
             |    from (select isp,province,room_id, 'others' as rtype, uid, split(others_str, ',') as others_list from tmp where size(split(others_str, ',')) = 6 ) r
             |) s
             |group by isp,province,room_id, rtype
             |;
           """.stripMargin
        )
        /*"jdbc:mysql://140.143.137.196:3306/kingcall"*/
        result.write.mode(SaveMode.Append).jdbc("jdbc:mysql://10.52.7.209:3306/monitor", "danmu_delay_2", mysqlConf)
      }
    )
    streaming.start()
    streaming.awaitTermination()
  }

  def dataPrepare(str:String): Tuple2[String,String] ={
    val p=JSON.parseObject(str.replace("message:",""))
    val s1:String=p.get("chat").toString.replaceAll("['\\[' '\\]']","")
    val s2:String=p.get("other").toString.replaceAll("['\\[' '\\]']","")
    (s1,s2)
  }
}
