package sparkDM.sparkSql.hive

import org.apache.spark.sql.hive.HiveContext

object ReadDemo {

  def main(args: Array[String]): Unit = {
    import org.apache.spark.SparkConf
    import org.apache.spark.SparkContext
    val conf = new SparkConf().setAppName("Spark-Hive").setMaster("local")
    val sc = new SparkContext(conf)

    val sqlContext = new HiveContext(sc)

    sqlContext.sql("CREATE TABLE IF NOT EXISTS src (key INT, value STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' ") //这里需要注意数据的间隔符

    sqlContext.sql("LOAD DATA INPATH '/user/liujiyu/spark/kv1.txt' INTO TABLE src  ")

    sqlContext.sql(" SELECT * FROM jn1").collect.foreach(println)
  }
}
