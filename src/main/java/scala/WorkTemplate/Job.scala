package scala.WorkTemplate

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SparkSession}
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils}

import scala.collection.mutable
import scala.util.{ConfigUtil, Logging}

/**
  * Created by Andy on 2017/5/12 0012.
  */
trait Job extends Logging{
  val paramsContainer = mutable.Map[String, Any]()
  var configs = mutable.Map[String, String]()

  def initJobConf(conf: String): mutable.Map[String, String] = ConfigUtil.readFile(conf)


  def getSparkSession(appName: String, parameters: Map[String, String] = Map()): SparkSession = {
    val sparkConf = new SparkConf()
    if (parameters.nonEmpty) {
      parameters.foreach(kv => sparkConf.set(kv._1, kv._2))
    }
    val sparkSession = SparkSession.builder().config(sparkConf).appName(appName)
    parameters.get("enable.hive") match {
      case Some(_) =>
        sparkSession.enableHiveSupport().getOrCreate()
      case None => sparkSession.getOrCreate()
    }
  }
}
