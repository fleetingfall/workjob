package util

import java.util.concurrent.{SynchronousQueue, ThreadPoolExecutor, TimeUnit}

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.execution.datasources.jdbc.{JDBCOptions, JdbcUtils}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import constant.JobConfigDef._
import constant.RedisDef._

import scala.collection.mutable

/**
  * 隐式转换工具类
  */
object ImplicitUtils {
  val pool = new ThreadPoolExecutor(10, Int.MaxValue, 60, TimeUnit.SECONDS, new SynchronousQueue[Runnable]())
  pool.allowCoreThreadTimeOut(true)

  //df增强
  implicit class DataFrameExtra(df: DataFrame) {
    def saveDFToHDFSWithOrc(mode: String, path: String, partitions: String*) = {
      df.repartition(1)
        .write.mode(mode)
        .partitionBy(partitions: _*)
        .orc(path)
    }

    def saveToMysql(df: DataFrame, jobConf: mutable.Map[String, String]) = {
      val params = Map("user" -> jobConf("mysql.user"),
        "url" -> jobConf("jdbc.url"),
        "password" -> jobConf("mysql.pwd"),
        "driver" -> jobConf("mysql.driver"))
      val opts = new JDBCOptions(params)
      JdbcUtils.saveTable(df, jobConf("jdbc.url"), "app_stats", opts)
    }
  }

  /**
    * spark streaming implicit工具类
    *
    * @param job
    */
  implicit class JobExtra(job: Job) {
    def run(f: => Unit) = {
      pool.submit(new Runnable {
        override def run(): Unit = {
          f
        }
      })
    }

    lazy val streamConfigs: Map[String, String] = {
      job.configs.filter(_._1.startsWith("stream")).map { case (k, v) => k.replace("stream.", "") -> v }.toMap
    }

    def startStreaming(f: (RDD[(String, String)]) => Unit) = {
      val ss = job.paramsContainer("sparkSession").asInstanceOf[SparkSession]
      val sc = ss.sparkContext
      val stc: StreamingContext = new StreamingContext(sc, Seconds(job.configs(JOB_DURATION).toInt))
      val recordDStream: InputDStream[(String, String)] = job.createDirectStream(stc, streamConfigs)

      recordDStream.foreachRDD(rdd => {
        try {
          if (rdd.count() > 0) {
            rdd.cache()
            f(rdd)
            job.saveOffset(rdd, streamConfigs(ZOOKEEPER_LIST), streamConfigs(ZOOKEEPER_PATH))
            rdd.unpersist()
          }
        } catch {
          case e: Exception =>
            job.saveErrorStatus(job.configs(JOB_NAME), e.getLocalizedMessage)
            e.printStackTrace()
        }
      })
      stc.start()
      stc.awaitTermination()
    }
  }


  implicit class SparkConfExtra(sparkConf: SparkConf) {
    require(sparkConf != null)

    def enableRedisSupport(parameters: Map[String, String]): SparkConf = {
      sparkConf.set(REDIS_HOST, parameters(REDIS_HOST))
        .set(REDIS_PORT, parameters(REDIS_PORT))
        .set(REDIS_TIMEOUT, parameters(REDIS_TIMEOUT))
    }
  }

  implicit def defineSchema(fieldsStr: String): StructType = {
    val fieldsList = fieldsStr.split(",").toList
    StructType(fieldsList.map(a => StructField(a, StringType, nullable = true)))
  }

}
