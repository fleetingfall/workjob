package scala.sparkSql

import org.apache.spark.sql.{DataFrame, SparkSession}


/*DF 的 API*/
object DataFramApi {
  val sparkSession:SparkSession=SparkSession.builder().appName("Base Demo").master("local[2]").getOrCreate()
  val DF=sparkSession.read.json("D:\\workingspace\\Code\\workjob\\src\\main\\resources\\json.txt")
  def main(args: Array[String]): Unit = {
    test4(DF)
  }
  def test1(DF:DataFrame): Unit ={
    DF.show()
  }
  def test2(DF:DataFrame): Unit ={
    DF.select("args").show()
  }
  /*档对某一列需要操作的时候用  $*/
  def test3(DF:DataFrame): Unit ={
    DF.select("args","client_ip").show()
  }

  def test4(DF:DataFrame): Unit ={
    DF.groupBy("args").count().show()
  }
}
