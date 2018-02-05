package scala.WorkTemplate

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import scala.util.Logging

/**
  * 通用steaming 模板
  */
trait StreamingTemplate extends Logging with Job with Serializable {
/*  private var ss: SparkSession = _

  def initTemplateDriver(config: String): mutable.Map[String, String] = {
    val configs = initJobConf(config)
    val sparkSession = getSparkSession(configs(JOB_NAME), configs.toMap)
    ss = sparkSession
    paramsContainer += ("sparkSession" -> sparkSession)
    val fieldsStr = configs("fields")
    val fieldsList = fieldsStr.split(",").toList
    val sql = configs("sql").replace("@", "from_unixtime").format(configs("fields"))
    configs += ("sql" -> sql)
    paramsContainer += ("fields.list" -> fieldsList)
    configs
  }

  def textFormat(iterator: Iterator[Row], fieldsList: List[String]): Iterator[(String, String, String, String)] = {
    val res = ArrayBuffer.empty[(String, String, String, String)]
    iterator.foreach(row => {
      val records = ListBuffer[String]()
      val valueMap = row.getValuesMap(fieldsList)
      fieldsList.foreach { field =>
        try {
          val value = valueMap.getOrElse(field, "null")
          value match {
            case "" => records.+=(null)
            case _ => records.+=(value)
          }
        } catch {
          case e: Exception =>
            e.printStackTrace()
            val msg = s"=text format error= record -> [ field:$field , values :${valueMap.mkString("|")} ,error msg:${e.getMessage} ]"
            WechatUtils.sendWechatMsg(configs("job.name"), msg)
        }
      }
      res.+=((row.getString(0), row.getString(1), row.getString(2), records.mkString("|")))
    })
    res.iterator
  }


  //隐式转换函数:定义表的schema
  implicit def defineSchema(fieldsStr: String): StructType = {
    val fieldsList = fieldsStr.split(",").toList
    StructType(fieldsList.map(a => StructField(a, StringType, nullable = true)))
  }

  /**
    * 业务处理函数
    *
    * @param rdd streaming rdd
    * @return
    */
  def templateProcess(rdd: RDD[(String, String)]): Any = {
    val ss = paramsContainer("sparkSession").asInstanceOf[SparkSession]
    val read = ss.read
    read.schema(configs("fields"))
    val cleaned = rdd.mapPartitions(cleanSymbols)
    read.json(cleaned).createOrReplaceTempView(configs("tmp.table"))
    steamingTransformAndSave()
    reportData(Map("topic" -> configs("stream.topic.name"), "count" -> s"${rdd.count()}"))
  }

  def steamingTransformAndSave(): DataFrame = {
    val sql = configs("sql")
    val im = ss.implicits
    import im._
    val fieldsList = paramsContainer("fields.list").asInstanceOf[List[String]]
    var df: DataFrame = null
    configs.get("partition.format") match {
      case Some(value) =>
        df = ss.sql(sql)
        val partitions = value.split(",")
        saveStreamingAsOrc(df, configs("save.path"), partitions)
      //        saveStreamingAsOrc(df)
      case None =>
        df = ss.sql(sql).mapPartitions(textFormat(_, fieldsList)).toDF("year", "month", "day", "record")
        saveAsStreamingText(df)
    }
    df
  }

  def saveStreamingAsOrc(df: DataFrame,
                         path: String,
                         partitionBy: Array[String],
                         saveMode: String = "append"): Unit = {
    df.write.mode(saveMode)
      .partitionBy(partitionBy: _*)
      .orc(path)
  }

  def saveAsStreamingText(df: DataFrame): Unit = {
    val path = configs("save.path")
    df.repartition(1)
      .write.mode("append")
      .partitionBy("year", "month", "day")
      .option("delimiter", "|")
      .text(path)
  }

  def saveStreamingAsOrc(df: DataFrame): Unit = {
    val path = configs("save.path")
    df.write.mode("append").partitionBy("day").orc(path)
  }

  def saveStreaming(df: DataFrame): Unit = {
    val path = configs("save.path")
    df.repartition(1)
      .write.mode("append")
      .partitionBy("day")
      .option("delimiter", "|")
      .text(path)
  }*/
}
