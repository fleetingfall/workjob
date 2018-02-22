package sparkDM.firstwork

import java.io.{File, FileWriter}
import util.ZKUtils

import com.alibaba.fastjson.JSON
import com.typesafe.config.ConfigFactory
import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.{Decoder, StringDecoder}
import org.apache.spark.{SparkConf, SparkException}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.{InputDStream, ReceiverInputDStream}
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaCluster, KafkaUtils}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.datanucleus.store.rdbms.connectionpool.ConnectionPool
import scalikejdbc.{DB, ResultSetCursor}

import scala.collection.immutable.HashMap
import scala.reflect.ClassTag


/**
  * 发现offset并没有维护到zookeeper上去，每次读取都是从头开始读
  */
object CombineStreaming {
  val sparkConf = new SparkConf().setAppName("SqlNetworkWordCount").setMaster("local[2]")
  sparkConf.set("spark.testing.memory", "2147480000")
  val scc=new StreamingContext(sparkConf,Seconds(2))
  scc.checkpoint("C:\\Users\\kingc\\Desktop\\tmp3")
  val zkQuorum="master:2181,slave1:2181,slave2:2181"
  import org.apache.spark.streaming.kafka.KafkaUtils

  implicit def String2Int(p:String)=p.toInt

  def main(args:Array[String]): Unit ={
    saveOffsetToZK2
  }

  def createDirectStream(configs:Map[String,String]): InputDStream[(String, String)] ={
    val fromOffsets = ZKUtils.readOffsets(configs)
    val recordDStream = if (fromOffsets.nonEmpty) {
      KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, (String, String)](scc, configs, fromOffsets, (mmd: MessageAndMetadata[String, String]) => (mmd.key, mmd.message))
    } else {
      val topics = Set(configs("topic.name"))
      KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](scc, configs, topics)
    }
    recordDStream
  }

  def test1(): Unit ={

    /*数据检查点，当发生死机时，可从上次失败的地方继续执行-------------->如何验证,还有就是这句代码所处的位置*/
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
    /*可以避免一个RDD有多个分区，有合并分区的意思*/
    /*inputRdd.repartition(1).saveAsTextFiles("C:\\Users\\PLUSH80702\\Desktop\\receive\\")*/
    scc.start()
    scc.awaitTermination()
    scc.stop()

  }

  /**
    * 状态维护的操作
    */
  def UpdateStateByKeyDemo(): Unit ={

    val addFunc = (values: Seq[Int], state: Option[Int]) => {
      val currentCount = values.sum

      val previousCount = state.getOrElse(0)

      Some(currentCount + previousCount)
    }



    val conf=new SparkConf().setMaster("local[2]").setAppName("kingcall")
    val sc=new StreamingContext(conf, Seconds(5))
    sc.checkpoint("checkPoint")
    val lines:ReceiverInputDStream[String]=sc.socketTextStream("master",9999)
    val pair=lines.flatMap(_.split(",")).map(x=>(x,1))
    /*只统计目前的*/
    val count=pair.reduceByKey((x,y)=>(x+y)).foreachRDD(x=>x.foreach(println _))

  /*  val totalWordCounts = pair.updateStateByKey[Int](addFunc)
    totalWordCounts.print()*/
    sc.start()
    sc.awaitTermination()
  }

  /**
    * foreachRDD 设计模式的使用
    * 如何正确高效的创建外部链接，对数据进行输出  假设我要将每一条数据输出到文件
    * 错误：因为这需要将连接对象序列化并从 driver 发送到 worker   正确的解决方案是在 worker 创建连接对象.  RDD级别上创建错误  record上创建低效
    * 愿景：创建一个链接池，来更高效的使用
    */
  def foreachRddDesign(): Unit ={
    val input=KafkaUtils.createStream(scc,zkQuorum,"kingcall_Group" ,Map("longzhuresty" -> 1),StorageLevel.MEMORY_ONLY)
    input.foreachRDD(rdd=>{
      rdd.foreachPartition(x=>{
        val fileWriter:FileWriter=new FileWriter(new File("C:\\Users\\kingc\\Desktop\\data.txt"),true)
        x.foreach(y=>{
          println(y)
          fileWriter.write(y.toString()+"\r\n")
        })
        fileWriter.close()
      })
    })
    scc.start()
    scc.awaitTermination()
    scc.stop(false)
  }

  /**
    *有消费组信息，但是发现但是发现offset节点下并没有对应主题的offset值，而且存在着随机现象
    */
  def offsetAboutAuto(): Unit ={
    val input=KafkaUtils.createStream(scc,zkQuorum,"kingcall_gp" ,Map("lk" -> 1),StorageLevel.MEMORY_ONLY)
    input.foreachRDD(rdd=>{
      rdd.foreach(println _)
      //   saveOffset(rdd)  这个方法在这里并不合适 org.apache.spark.rdd.BlockRDD cannot be cast to org.apache.spark.streaming.kafka.HasOffsetRanges
    })
    scc.start()
    scc.awaitTermination()
    scc.stop(false)
  }


  /**
    * 不止没有在zookeeper上保留offset信息 连 消费组信息都没有
    * 每次都从最新的地方开始读
    */
  def offsetAboutDirect(): Unit ={
    val kafkaParams = Map(
      "metadata.broker.list" -> "master:9092,slave1:9092,slave2:9092"
    )
    val topics = Set("lk")
    /*查没有过期的用法是什么，而且这个方法好像在新版的Kafka好像也不支持——不是不支持是方法变了*/
    val input=KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](scc,kafkaParams,topics )
    input.foreachRDD(rdd=>{
      rdd.foreach(println _)
      })
    scc.start()
    scc.awaitTermination()
    scc.stop(false)
  }

  def saveOffsetToZK(): Unit ={
    val kafkaParams = Map(
      "metadata.broker.list" -> "master:9092,slave1:9092,slave2:9092",
      "group.id" -> "directGP",
      "zookeeper.connect"->zkQuorum
    )
    val topics = Set("oso")
    val kafkaManager:KafkaManager=new KafkaManager(kafkaParams)

    val input=kafkaManager.createDirectStream[String, String, StringDecoder, StringDecoder](scc,kafkaParams,topics )
    input.foreachRDD(rdd=>{
      rdd.foreach(println _)
      kafkaManager.updateZKOffsets(rdd)
    })
    scc.start()
    scc.awaitTermination()
    scc.stop(false)
  }

  /**
    * 有缺陷的代码，只有维护,没有读取
    */

  def saveOffsetToZK2(): Unit ={
    val kafkaParams = Map(
      "metadata.broker.list" -> "master:9092,slave1:9092,slave2:9092",
      "zk.list"->zkQuorum,
      "zk.path"->"/consumers/directDM/offsets/directDM",
      "group.id"->"directDM",
      "topic.name"->"direcrDM"
    )
    val topics = Set("directDM")
    /*查没有过期的用法是什么，而且这个方法好像在新版的Kafka好像也不支持——不是不支持是方法变了*/
    val input=createDirectStream(kafkaParams)
    input.foreachRDD(rdd=>{
      rdd.foreach(println _)
      saveOffset(rdd,kafkaParams)
    })
    scc.start()
    scc.awaitTermination()
    scc.stop(false)
  }

  /**
    * 保存offset 实现方法是纯粹的借助 ZkClient完成的
    * @param rdd
    * @param zkList
    * @param savePath  参数是参考zookeeper对offset 节点的命名特点  /consumers/消费组/offsets/topic         其中下面还要到分区，但是分区信息是包含在了RDD中的
    */
  def saveOffset(rdd: RDD[(String, String)],parameter:Map[String,String]):Unit= {
    val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
    println(offsetRanges.mkString("\t"))
   ZKUtils.saveOffsets(parameter.get("zk.list").get,parameter.get("zk.path").get,offsetRanges)
  }
}


/**
  * 一个简单的维护offset的类  是通过kafka的一个API来维护的
  * KafkaCluster类用于建立和Kafka集群的链接相关的操作工具类，我们可以对Kafka中Topic的每个分区设置其相应的偏移量
  * Map((topicAndPartition, offsets.untilOffset)),然后调用KafkaCluster类的setConsumerOffsets方法去更新Zookeeper里面的信息，这样我们就可以更新Kafka的偏移量，
  * @param kafkaParams
  */
class KafkaManager(val kafkaParams: Map[String, String]) extends Serializable {

  private val kc = new KafkaCluster(kafkaParams)

  /**
    * 创建数据流 通过此方法创建的流是每次都会去zoo
    *
    * @param ssc
    * @param kafkaParams
    * @param topics
    * @tparam K
    * @tparam V
    * @tparam KD
    * @tparam VD
    * @return
    */
  def createDirectStream[K: ClassTag, V: ClassTag, KD <: Decoder[K] : ClassTag, VD <: Decoder[V] : ClassTag](
    ssc: StreamingContext,
    kafkaParams:Map[String, String],
    topics: Set[String]
  ): InputDStream[(K, V)] = {
    val groupId = kafkaParams.get("group.id").get
    //从zookeeper上读取offset开始消费message
    val partitionsE = kc.getPartitions(topics)
    if (partitionsE.isLeft)
      throw new SparkException(s"get kafka partition failed: ${partitionsE.left.get}")
    val partitions = partitionsE.right.get
    val consumerOffsetsE = kc.getConsumerOffsets(groupId, partitions)
    if (!consumerOffsetsE.isLeft) {
      val consumerOffsets = consumerOffsetsE.right.get
      KafkaUtils.createDirectStream[K, V, KD, VD, (K, V)](
        ssc, kafkaParams, consumerOffsets, (mmd: MessageAndMetadata[K, V]) => (mmd.key, mmd.message))
    } else {
      val p = kafkaParams + ("auto.offset.reset" -> "largest")
      KafkaUtils.createDirectStream(ssc, p, topics)
    }
  }


  /**
    * 更新zookeeper上的消费offsets
    *
    * @param rdd
    */
  def updateZKOffsets(rdd: RDD[(String, String)]): Unit = {
    val groupId = kafkaParams.get("group.id").get
    val offsetsList = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
    // OffsetRange(topic: 'oso', partition: 0, range: [581 -> 583]) 其实对一个RDD而言一个这样的对象

    for (offsets <- offsetsList) {
      val topicAndPartition = TopicAndPartition(offsets.topic, offsets.partition)
      val o = kc.setConsumerOffsets(groupId, HashMap((topicAndPartition, offsets.untilOffset)))
      if (o.isLeft) {
        println(s"Error updating the offset to Kafka cluster: ${o.left.get}")
      }
    }
  }
}


