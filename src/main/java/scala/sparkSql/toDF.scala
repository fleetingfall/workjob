package scala.sparkSql

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

/*
* 实践告诉我case class 最好写在外面
* */
case class people(id:String,name:String,age:String)


object toDF {
  val sparkSession:SparkSession=SparkSession.builder().appName("Base Demo").master("local[2]").getOrCreate()

  def main(args: Array[String]): Unit = {
    jsonObjToDF(sparkSession)
  }
  def StructTypeMethod(sparkSession:SparkSession): Unit ={
    /*
    *  字段比较少的时候可以采用下面的方式书写
    *  val testSchema = StructType(Array(StructField("ID", StringType, true), StructField("Name", StringType, true), StructField("District", StringType, true)))
    * */
    val schemaString = "id name age"
    val schema = StructType(schemaString.split(" ").map(fieldName ⇒ StructField(fieldName, StringType, true)))
    /* 虽然有这样一个函数，sparkSession.read.textFile()，但是你不要用，会出错误*/
    val rowRDD = sparkSession.sparkContext.textFile("D:\\workingspace\\Code\\workjob\\src\\main\\resources\\people.txt").map(x => x.split(",")).map( p => Row(p(0), p(1), p(2)))
    val testDF = sparkSession.createDataFrame(rowRDD, schema)
    testDF.createOrReplaceTempView("test")
    val resultDF=sparkSession.sql("SELECT * FROM test")
    resultDF.show()
  }

  /*：隐式转换会将含有 case 对象的 RDD 转换为 DataFrame */
  def casemethod(sparkSession:SparkSession): Unit ={
    import sparkSession.sqlContext.implicits._
    val rowRDD = sparkSession.sparkContext.textFile("D:\\workingspace\\Code\\workjob\\src\\main\\resources\\people.txt")
    val caseRDD=rowRDD.map(x=>x.split(",")).map(x=>people(x(0),x(1),x(2))).toDF().show()
  }
  /*
  * 将本地的json读成DF
  * */
  def jsonDF(sparkSession:SparkSession): Unit ={
    sparkSession.read.json("D:\\workingspace\\Code\\workjob\\src\\main\\resources\\json.txt").toDF().show()
  }

  /*
  * json----->
  * */
  def jsonObjToDF(sparkSession:SparkSession): Unit ={
    val JSONRDD=sparkSession.sparkContext.textFile("D:\\workingspace\\Code\\workjob\\src\\main\\resources\\json.txt")
    JSONRDD
  }

}
