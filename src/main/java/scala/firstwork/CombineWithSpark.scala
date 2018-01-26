package scala.firstwork

import java.util

import com.alibaba.fastjson.JSON
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapred.TextOutputFormat
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka.KafkaUtils

object CombineWithSpark {
  implicit def String2Int(p:String)=p.toInt

  def main(args:Array[String]){
    println("开始接收数据了")
    val sparkConf = new SparkConf().setAppName("SqlNetworkWordCount").setMaster("local[2]")
    sparkConf.set("spark.testing.memory", "2147480000")
    val scc=new StreamingContext(sparkConf,Seconds(2))

      scc.checkpoint("C:\\Users\\PLUSH80702\\Desktop\\tmp")
    /*数据检查点，当发生死机时，可从上次失败的地方继续执行-------------->如何验证,还有就是这句代码所处的位置*/
    val zkQuorum="10.52.7.20:2181,10.52.7.21:2181,10.52.7.22:2181,master:2181"
    import org.apache.spark.streaming.kafka.KafkaUtils

    val inputRdd:ReceiverInputDStream[(String,String)] = KafkaUtils.createStream(scc,zkQuorum,"user-behavior-topic-message-consumer-group" ,Map("longzhuresty" -> 1)
      ,StorageLevel.MEMORY_ONLY)

    /*
      第一个参数是StreamingContext实例;
      第二个参数是ZooKeeper集群信息(接受Kafka数据的时候会从ZooKeeper中获得Offset等元数据信息)
      第三个参数是Consumer Group
      第四个参数是消费的Topic以及并发读取Topic中Partition的线程数
    */

    /*对接收到的数据进行判断处理  新的问题spark中流处理到底是从哪里开始循环的     发现这个方法只被调用了一次*/

   /* val path:String=SaveToHDFS.createFolderAndGetPath()*/

    inputRdd.foreachRDD(rdd=>{
      if (rdd.count()>0){
        var name=""
        /*如果经常在每间隔5秒钟没有数据的话不断的启动空的Job其实是会造成调度资源的浪费，因为并没有数据需要发生计算，所以实例的企业级生成环境的代码在具体提交Job前会判断是否有数据，如果没有的话就不再提交Job*/
        rdd.foreach(x=>{
          var tmpobj=JSON.parseObject(x._2)
          name=tmpobj.get("host")+""
          println("---------------"+name)
          var conetxt:String=tmpobj.get("client_ip")+"\t"+tmpobj.get("is_blocked")+"\t"+tmpobj.get("args")+"\t"+tmpobj.get("status")+"\t"+tmpobj.get("uid")+"\t"+name+"\r\n"
          SaveToLocal.saveFile(SaveToLocal.createFolderAndGetPath,conetxt,name)
          tmpobj
        })
      }
    })
    /*inputRdd.repartition(1).saveAsTextFiles("C:\\Users\\PLUSH80702\\Desktop\\receive\\")*/
    scc.start()
   scc.awaitTermination()
  }
}
