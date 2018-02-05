package scala.WorkTemplate

import scala.collection.mutable

/**
  * 实时任务常用配置类
  */
object StreamJobConfigs extends Serializable {

  var ZOOKEEPER_LIST: String = _
  var ZOOKEEPER_PATH: String = _
  var KAFKA_BROKERS: String = _
  var GROUP_ID: String = _

  var TOPIC_NAME: String = _
  var JOB_NAME: String = _
  var JOB_DURATION: Int = _
  var ALL_CONFIG = mutable.Map[String, String]()
  var STREAM_CONFIG = Map[String, String]()
  var OTHER_CONFIG = Map[String, String]()

  lazy val TOPIC_SET = TOPIC_NAME.split(",").toSet
}
