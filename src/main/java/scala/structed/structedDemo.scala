package scala.structed

object structedDemo {
  /*
  * Structured Streaming （结构化流）是一种基于 Spark SQL 引擎构建的可扩展且容错的 stream processing engine （流处理引擎）
  * 您可以以静态数据表示批量计算的方式来表达 streaming computation （流式计算）。 Spark SQL 引擎将随着 streaming data 持续到达而增量地持续地运行，并更新最终结果。
  * */
  import org.apache.spark.sql.functions._
  import org.apache.spark.sql.SparkSession
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("Base Demo").master("local[2]").getOrCreate()
    val lines = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 9999)
      .load()
    import spark.implicits._
    val words = lines.as[String].flatMap(_.split(" "))
    // 生成正在运行的 word count
    val wordCounts = words.groupBy("value").count()

    val query = wordCounts.writeStream
      .outputMode("complete")
      .format("console")
      .start()
    query.awaitTermination()

  }

}
