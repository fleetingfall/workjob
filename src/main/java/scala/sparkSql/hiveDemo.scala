package scala.sparkSql

import org.apache.spark.sql.SparkSession

object hiveDemo {
  val spark = SparkSession
    .builder()
    .appName("Spark Hive Example")
    .config("spark.sql.warehouse.dir", "warehouseLocation")
    .enableHiveSupport()
    .getOrCreate()

}
