package util

import java.util.Properties

import kafka.common.TopicAndPartition
import kafka.utils._
import org.I0Itec.zkclient.ZkClient
import org.I0Itec.zkclient.exception.{ZkNoNodeException, ZkNodeExistsException}
import org.apache.spark.streaming.kafka.OffsetRange

import scala.collection.JavaConversions._
import scala.collection.{Seq, mutable}


/**
  *
  * ZkClient是由Datameer的工程师开发的开源客户端，对Zookeeper的原生API进行了包装，实现了超时重连、Watcher反复注册等功能。
  * kafka本身就有一套关于zk的工具类 kafka.utils.{VerifiableProperties, ZKConfig, ZKGroupTopicDirs, ZkUtils}
  *
  * 这个工具类
  */
object ZKUtils extends Serializable with Logging {
  def createZKClient(zkQuorum: String): ZkClient = {
    val props = new Properties
    props.put("zookeeper.connect", zkQuorum)
    props.put("zookeeper.connection.timeout.ms", "30000")
    val zkConfig = new ZKConfig(new VerifiableProperties(props))
    new ZkClient(zkConfig.zkConnect, zkConfig.zkSessionTimeoutMs, zkConfig.zkConnectionTimeoutMs, ZKStringSerializer)
  }

  /**
    * 返回指定队列的partitions
    *
    * @param client
    * @param topicSeq
    * @return
    */
  def getPartitionsForTopics(client: ZkClient, topicSeq: Seq[String]): mutable.Map[String, Seq[Int]] = {
    ZkUtils.getPartitionsForTopics(client, topicSeq)
  }

  private def toPath(topic: String, groupId: String) = {
    val groupDirs = new ZKGroupTopicDirs(groupId, topic)
    s"${groupDirs.consumerGroupDir}/offsets/" + topic
  }

  def fetchLastOffsets(client: ZkClient, topic: String, groupId: String): Option[Map[TopicAndPartition, Long]] = {
    try {
      Some(client.readData(toPath(topic, groupId)))
    } catch {
      case e: ZkNoNodeException =>
        e.printStackTrace()
        None
      case e2: Throwable => throw e2
    }
  }

  def updateLastOffsets(client: ZkClient, topic: String, groupId: String, offsetRange: Array[OffsetRange]) {
    val data = offsetRange.map { x => TopicAndPartition(x.topic, x.partition) -> x.untilOffset }.toMap

    val path = toPath(topic, groupId)
    try {
      client.writeData(path, data)
    } catch {
      case e: ZkNoNodeException =>
        e.printStackTrace()
        createParentPath(client, path)
        try {
          client.createPersistent(path, data)
        } catch {
          case e: ZkNodeExistsException =>
            e.printStackTrace()
            client.writeData(path, data)
          case e2: Throwable => throw e2
        }
      case e2: Throwable => throw e2
    }
  }

  private def createParentPath(client: ZkClient, path: String): Unit = {
    val parentDir = path.substring(0, path.lastIndexOf('/'))
    if (parentDir.length != 0)
      client.createPersistent(parentDir, true)
  }

  def getCorrectOffset(fromOffset: Map[TopicAndPartition, Long], configs: Map[String, String]): Map[TopicAndPartition, Long] = {
    val topic = configs("topic.name")
    val brokerList = configs("metadata.broker.list")
    val groupId = configs("group.id")
    // lastest offsets
    val lastMap = KafkaOffsetTool.getInstance.getLastOffset(brokerList, List(topic), groupId)
    // earliest offsets
    val earlierMap = KafkaOffsetTool.getInstance.getEarliestOffset(brokerList, List(topic), groupId)
    logWarning(s"last offset map :$lastMap ,earlier Map :$earlierMap")
    fromOffset.map {
      case (t, offset) =>
        val lastOffset: Long = lastMap.get(t)
        val earliestOffset: Long = earlierMap.get(t)
        logWarning(s"last offset : $lastOffset , earliest Offset :$earliestOffset")
        if (offset > lastOffset || offset < earliestOffset) {
          (t, earliestOffset)
        } else {
          (t, offset)
        }
    }
  }

  // Read the previously saved offsets from Zookeeper
  def readOffsets(configs: Map[String, String]): Map[TopicAndPartition, Long] = {
    import scala.collection.JavaConversions._
    val zkCli = createZKClient(configs("zk.list"))
    val topic = configs("topic.name")
    logInfo(s"Reading offsets of $topic from Zookeeper")
    val stopwatch = new Stopwatch()
    val (offsetsRangesStrOpt, _) = ZkUtils.readDataMaybeNull(zkCli, configs("zk.path"))
    zkCli.close()
    if (offsetsRangesStrOpt.isDefined) {
      val offsetsRangesStr = offsetsRangesStrOpt.get
      logWarning(s"Read offset ranges: $offsetsRangesStr")
      val offsets = offsetsRangesStr.split(",")
        .map(_.split(":"))
        .map { case Array(partitionStr, offsetStr) => TopicAndPartition(topic, partitionStr.toInt) -> offsetStr.toLong }
        .toMap
      logWarning("Done reading offsets from Zookeeper. Took " + stopwatch)
      //        Some(offsets)
      getCorrectOffset(offsets, configs)
    } else if (configs.contains("auto.offset.reset")) Map() //判断是否指定从最开始的读.
    else {
      logWarning("using latest offset")
      val topics = configs("topic.name").split(",").toList
      val brokerList = configs("metadata.broker.list")
      val groupId = configs("group.id")
      val lastMap = KafkaOffsetTool.getInstance.getLastOffset(brokerList, topics, groupId)
      lastMap.map { case (t, p) => t -> p.toLong }.toMap
    }
  }

  def checkOffsets(fromOffset: Map[TopicAndPartition, Long])(implicit configs: Map[String, String]): Map[TopicAndPartition, Long] = {
    val topics = configs("topic.name").split(",").toList
    val brokerList = configs("metadata.broker.list")
    val groupId = configs("group.id")
    // lastest offsets
    val lastMap = KafkaOffsetTool.getInstance.getLastOffset(brokerList, topics, groupId)
    // earliest offsets
    val earlierMap = KafkaOffsetTool.getInstance.getEarliestOffset(brokerList, topics, groupId)
    logWarning(s"last offset map :$lastMap ,earlier Map :$earlierMap")
    fromOffset.map {
      case (t, offset) =>
        val lastOffset: Long = lastMap.get(t)
        val earliestOffset: Long = earlierMap.get(t)
        logWarning(s"last offset : $lastOffset , earliest Offset :$earliestOffset")
        if (offset > lastOffset || offset < earliestOffset) {
          (t, earliestOffset)
        } else {
          (t, offset)
        }
    }
  }


  // Save the offsets back to Zookeeper
  def saveOffsets(zkList: String, zkPath: String, offsetsRanges: Array[OffsetRange]): Unit = {
    val zkClient = ZKUtils.createZKClient(zkList)
    try {
      //      logWarning(s"Saving offsets to Zookeeper : [ ${offsetsRanges.mkString("\n")} ]")
      val offsetsRangesStr = offsetsRanges.map(offsetRange => s"${offsetRange.partition}:${offsetRange.fromOffset}")
        .mkString(",")
      ZkUtils.updatePersistentPath(zkClient, zkPath, offsetsRangesStr)
    } catch {
      case e: Exception =>
        e.printStackTrace()
    } finally {
      zkClient.close()
    }
  }

  def saveOffsetAsJson(zkList: String, zkPath: String, offsetsRanges: Array[OffsetRange]): Unit = {
    logWarning("saving offsets as json to Zookeeper")
    offsetsRanges.foreach(offsetRange => logWarning(s"Using $offsetRange"))
    val zkClient = ZKUtils.createZKClient(zkList)
    try {
      val m = offsetsRanges.map(offsetRange => offsetRange.topic -> s"${offsetRange.partition}:${offsetRange.fromOffset}").toMap
      val json = JsonUtils.writeMapToJson(m)
      ZkUtils.updatePersistentPath(zkClient, zkPath, json)
    } catch {
      case e: Exception =>
        e.printStackTrace()
    } finally {
      zkClient.close()
    }
  }

  // very simple stop watch to avoid using Guava's one
  class Stopwatch {
    private val start = System.currentTimeMillis()

    override def toString = (System.currentTimeMillis() - start) + " ms"
  }

}
