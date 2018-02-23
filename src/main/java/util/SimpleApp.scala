/*
package util

import java.sql.ResultSet

import com.typesafe.config.ConfigFactory
import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import org.apache.spark.{SparkConf, SparkException}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaCluster, KafkaUtils}
import org.datanucleus.store.rdbms.connectionpool.ConnectionPool
import scalikejdbc.{DB, ResultSetCursor}

object SimpleApp {
  def main(args: Array[String]): Unit = {
    val conf = ConfigFactory.load
    // 加载工程resources目录下application.conf文件，该文件中配置了databases信息，以及topic及group消息
    val kafkaParams = Map[String, String]("metadata.broker.list" -> conf.getString("kafka.brokers"),
      "group.id" -> conf.getString("kafka.group"),
      "auto.offset.reset" -> "smallest")
    val jdbcDriver = conf.getString("jdbc.driver")
    val jdbcUrl = conf.getString("jdbc.url")
    val jdbcUser = conf.getString("jdbc.user")
    val jdbcPassword = conf.getString("jdbc.password")
    val topic = conf.getString("kafka.topics")
    val group = conf.getString("kafka.group")
    val ssc = setupSsc(kafkaParams, jdbcDriver, jdbcUrl, jdbcUser, jdbcPassword, topic, group)()
    ssc.start()
    ssc.awaitTermination()
  }
  def createStream(taskOffsetInfo: Map[TopicAndPartition, Long], kafkaParams: Map[String, String], conf:SparkConf, ssc: StreamingContext, topics:String):InputDStream[_] = {
    //若taskOffsetInfo 不为空， 说明这不是第一次启动该任务, database已经保存了该topic下该group的已消费的offset, 则对比kafka中该topic有效的offset的最小值和数据库保存的offset，去比较大作为新的offset.
    if(taskOffsetInfo.size != 0) {
      val kc = new KafkaCluster(kafkaParams)
      val earliestLeaderOffsets = kc.getEarliestLeaderOffsets(taskOffsetInfo.keySet)
      if (earliestLeaderOffsets.isLeft)
        throw new SparkException("get kafka partition failed:")
      val earliestOffSets = earliestLeaderOffsets.right.get
      val offsets = earliestOffSets.map(r => new TopicAndPartition(r._1.topic, r._1.partition) -> r._2.offset.toLong)
      val newOffsets = taskOffsetInfo.map(r => {
        val t = offsets(r._1)
        if (t > r._2) {
          r._1 -> t
        }
        else {
          r._1 -> r._2
        }
      }
      )
      val messageHandler = (mmd: MessageAndMetadata[String, String]) => 1L
      KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, Long](ssc, kafkaParams, newOffsets, messageHandler)
      // val stream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder]
    }
    else { val topicSet = topics.split(",").toSet
      KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams,topicSet) }
  }
  def setupSsc( kafkaParams: Map[String, String],
                jdbcDriver: String, jdbcUrl: String,
                jdbcUser: String, jdbcPassword: String,
                topics:String, group:String )(): StreamingContext = {
    val conf = new SparkConf()
      .setMaster("mesos://10.142.113.239:5050")
      .setAppName("offset")
      .set("spark.worker.timeout", "500")
      .set("spark.cores.max", "10")
      .set("spark.streaming.kafka.maxRatePerPartition", "500")
      .set("spark.rpc.askTimeout", "600s")
      .set("spark.network.timeout", "600s")
      .set("spark.streaming.backpressure.enabled", "true")
      .set("spark.task.maxFailures", "1")
      .set("spark.speculationfalse", "false")
    val ssc = new StreamingContext(conf, Seconds(5))
    SetupJdbc(jdbcDriver, jdbcUrl, jdbcUser, jdbcPassword) // connect to mysql
    // begin from the the offsets committed to the database
    val fromOffsets = DB.readOnly {implicit session =>
      s"select topic, part, offset from streaming_task where group_id=$group".
        map { resultSet =>
          new TopicAndPartition(resultSet.String(1), resultSet.getInt(2)) -> resultSet.getLong(3)
        }.list.apply().toMap
    }
    val stream = createStream(fromOffsets, kafkaParams, conf, ssc, topics)
    stream.foreachRDD { rdd => if(rdd.count != 0){
      //you task
      val t = rdd.map(record => (record, 1))
      val results = t.reduceByKey {_+_}.collect//persist the offset into the database
      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      DB.localTx { implicit session =>
        offsetRanges.foreach { osr =>
          s"""replace into streaming_task values(${osr.topic}, ${group}, ${osr.partition}, ${osr.untilOffset})""".update.apply()
          if(osr.partition == 0){ println(osr.partition, osr.untilOffset) }
        }
      }
    }
    }
    ssc
  }
}

object SetupJdbc {
  def apply(driver: String, host: String, user: String, password: String): Unit = {
    Class.forName(driver)
    ConnectionPool.singleton(host, user, password) }
}

*/
