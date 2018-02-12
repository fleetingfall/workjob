package sparkDM.sparkSql.structed

import com.alibaba.fastjson.JSON
import org.apache.spark.sql.{DataFrame, Row}

import sparkDM.firstwork.Bean

/**
  * 结构化流就是 Streaming DataFrame
  */

object structedDemo {
  /*
  * Structured Streaming （结构化流）是一种基于 Spark SQL 引擎构建的可扩展且容错的 stream processing engine （流处理引擎）
  * 您可以以静态数据表示批量计算的方式来表达 streaming computation （流式计算）。 Spark SQL 引擎将随着 streaming data 持续到达而增量地持续地运行，并更新最终结果。
  * 强大之处也在于  增量运行
  * 系统通过 checkpointing （检查点） 和 Write Ahead Logs （预写日志）来确保 end-to-end exactly-once （端到端的完全一次性）
  * 你可以当做静态数据来处理，直接忽略流的概念
  *
  * Structured Streaming 的关键思想是将 live data stream （实时数据流）视为一种正在不断 appended （附加）的表。这形成了一个与 batch processing model
  * （批处理模型）非常相似的新的 stream processing model （流处理模型）
  *
  * 从运行的表象上看结果不更新就不输出，好像就在静静的监控着一样
  * */
  import org.apache.spark.sql.functions._
  import org.apache.spark.sql.SparkSession
  val spark = SparkSession.builder().appName("Base Demo").master("local[2]").config("spark.sql.streaming.checkpointLocation","C:\\Users\\PLUSH80702\\Desktop\\check").getOrCreate()
  def main(args: Array[String]): Unit = {
    writeDFtoKafka


  }

  /**
    * 创建SDF
    * spark.readStream  大多数方法返回的对象都是DF
    * 支持的数据源
    *   1.  File   text ， csv ， json ， parquet
    *   2.  Kafka
    *   3.  socket 用于测试
    */

  def createSDFByFile(): Unit ={
    val reader=spark.readStream
    /*去底层查看该方法，还是 format 方法的调用*/
    val lines=reader.text("src/main/resources/")
    import spark.implicits._
    //                                        转换
    val words = lines.as[String].flatMap(_.split(" "))
    // 生成正在运行的 word count 这里使用的是 DF的 API 吗

    val wordCounts = words.groupBy("value").count()
    //                                        结果
    val query = wordCounts.writeStream
      .outputMode("complete")
      .format("console")
      .start()
    query.awaitTermination()

  }
  /*
  * DF 有个start方法 是怎么用的
  * 每行都包含如下信息：key:binary   value:binary   topic:string      partition:int   offset:long  timestamp:long   timestampType:int
  * 新的问题是：如何还原成包含多列信息的DF
  * */
  def createSDFByKafka(): Unit ={
    /*你甚至可以指定kafka的offset*/
    val lines:DataFrame=spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", "master:9092")// "host1:port1,host2:port2" 可以添加多台机器
        .option("subscribe", "longzhuresty")// 正则订阅.option("subscribePattern", "topic.*")   订阅多个topic  option("subscribe", "topic1,topic2")
        .load()
    import spark.implicits._
    val DF=lines.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)").as[(String, String)]
    val DF2=DF.select("value")
    val result=DF2.writeStream.outputMode("append").format("console").start()
    result.awaitTermination()

  }

  /**
    * 这个方法失败了
    */
  def writeDFtoKafka(): Unit ={
    import spark.implicits._
    val lines:DataFrame=spark.readStream.format("kafka")
      .option("kafka.bootstrap.servers", "master:9092")// "host1:port1,host2:port2" 可以添加多台机器
      .option("subscribe", "longzhuresty")// 正则订阅.option("subscribePattern", "topic.*")   订阅多个topic  option("subscribe", "topic1,topic2")
      .load()
    val DF=lines.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
      .writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "slav1:9092,slave2:9092")
      .option("topic", "topic1")
      .start()
    DF.awaitTermination()

  }

  /**
    *

    Complete Mode（完全模式） - 整个更新的 Result Table 将被写入外部存储。由 storage connector （存储连接器）决定如何处理整个表的写入。

    Append Mode（附加模式） - 只有 Result Table 中自上次触发后附加的新 rows（行） 将被写入 external storage （外部存储）。这仅适用于不期望更改 Result Table 中现有行的查询。

    Update Mode（更新模式） - 只有自上次触发后 Result Table 中更新的 rows （行）将被写入 external storage （外部存储）（从 Spark 2.1.1 之后可用）
    */

  def worldCount: Unit ={
    //streaming DataFrame   即每一行都是一个DF   输入
    val lines = spark.readStream.format("socket").option("host", "master").option("port", 9999).load()
    import spark.implicits._
    //                                        转换
    val words = lines.as[String].flatMap(_.split(" "))   //默认是将value转换吗？因为同事转两列时候报错，必须指明列名
    // 生成正在运行的 word count 这里使用的是 DF的 API 吗

    val wordCounts = words.groupBy("value").count()
    //                                        结果
    val query = wordCounts.writeStream
      .outputMode("complete")
      .format("console")
      .start()
    query.awaitTermination()
  }

}
