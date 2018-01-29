package scala.sparkSql

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object toDF {
  val sparkSession:SparkSession=SparkSession.builder().appName("Base Demo").master("local[2]").getOrCreate()

  def main(args: Array[String]): Unit = {
    val schemaString = "id name age"
    val schema = StructType(schemaString.split(" ").map(fieldName ⇒ StructField(fieldName, StringType, true)))
/*    val rowRDD=sparkSession.read.textFile("D:\\workingspace\\Code\\workjob\\src\\main\\resources\\people.txt").map(_.split(",")).map(x=>Row(x(0),x(1),x(2)))
    sqlContext.createDataFrame(rowRDD, schema).show()*/
  }


}
