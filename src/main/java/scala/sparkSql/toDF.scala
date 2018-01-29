package scala.sparkSql

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

object toDF {
  val sparkSession:SparkSession=SparkSession.builder().appName("Base Demo").master("local[2]").getOrCreate()

  def main(args: Array[String]): Unit = {
    StructTypeMethod(sparkSession)
  }
  def StructTypeMethod(sparkSession:SparkSession): Unit ={
    /*
    *  字段比较少的时候可以采用下面的方式书写
    *  val testSchema = StructType(Array(StructField("ID", StringType, true), StructField("Name", StringType, true), StructField("District", StringType, true)))
    * */
    val schemaString = "id name age"
    val schema = StructType(schemaString.split(" ").map(fieldName ⇒ StructField(fieldName, StringType, true)))
    /* 虽然有这样一个函数，sparkSession.read.textFile()，但是你不要用，会出错误*/
    val rowRDD = sparkSession.sparkContext.textFile("D:\\workingspace\\code\\IDEA\\workjob\\src\\main\\resources\\people.txt").map(x => x.split(",")).map( p => Row(p(0), p(1), p(2)))
    val testDF = sparkSession.createDataFrame(rowRDD, schema)
    testDF.createOrReplaceTempView("test")
    val resultDF=sparkSession.sql("SELECT * FROM test")
    resultDF.show()
  }

  /*：隐式转换会将含有 case 对象的 RDD 转换为 DataFrame */
  def casemethod(): Unit ={

  }


}
