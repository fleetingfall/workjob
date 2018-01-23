package scala.firstwork

import com.alibaba.fastjson.JSON
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka.KafkaUtils

object CombineWithSpark {
  implicit def String2Int(p:String)=p.toInt
  def main(args:Array[String]){
    println("开始接收数据了")
    val sparkConf = new SparkConf().setAppName("SqlNetworkWordCount").setMaster("local[2]")
    sparkConf.set("spark.testing.memory", "2147480000")
    val scc=new StreamingContext(sparkConf,Seconds(2))
   /* scc.checkpoint("C:\\Users\\PLUSH80702\\Desktop\\tmp")*/
   val zkQuorum="192.168.10.74:2181,192.168.10.75:2181,192.168.10.76:2181,localhost:2181"
    val inputRdd = KafkaUtils.createStream(scc,zkQuorum,"user-behavior-topic-message-consumer-group" ,Map("kingcall" -> 1)
      ,StorageLevel.MEMORY_ONLY)
    /*对接收到的数据进行判断处理，主要是文件夹操作，先存储在本地，每隔一段时间上传到hdfs文件系统*/
    inputRdd.foreachRDD(rdd=>{
      if (rdd.count()>0){
        rdd.foreach(x=>{
          var tmpobj=JSON.parseObject(x._2)
          println(tmpobj)
         /* var name:String=tmpobj.get("host")+""
          var conetxt:String=tmpobj.get("client_ip")+"\t"+tmpobj.get("is_blocked")+"\t"+tmpobj.get("args")+"\t"+tmpobj.get("status")+"\t"+tmpobj.get("uid")+"\t"+name+"\r\n"
          SaveToLocal.saveFile(conetxt,name)*/

        })
      }
    })

    scc.start()
    scc.awaitTermination()
  }
}
