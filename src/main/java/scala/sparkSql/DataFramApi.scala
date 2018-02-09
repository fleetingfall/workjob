package scala.sparkSql

import org.apache.spark.sql.{DataFrame, SparkSession}


/*DF 的 API*/
object DataFramApi {
  val sparkSession:SparkSession=SparkSession.builder().appName("Base Demo").master("local[2]").getOrCreate()
  val DF=sparkSession.read.json("src/main/resources/json.txt")
  def main(args: Array[String]): Unit = {
    castDemo(DF)
  }
  def test1(DF:DataFrame): Unit ={
    DF.show()
  }

  def dropColumn(DF:DataFrame): Unit ={
    DF.drop("args").show(3,false)
    DF.drop("args","client_ip").show(3,false)
    DF.drop(DF("args")).show(3,false)
  }

  def selectColumn(DF:DataFrame): Unit ={
    DF.select("args").show()
    //没有达到理想的效果
    DF.select(DF( "args" ), DF( "args")+"     lWA" ).show(false)
    /*
    * jdbcDF.apply("id")
    * jdbcDF("id")
    * 这两种方法返回都是 column 类型的
    * */
  }
  /*档对某一列需要操作的时候用*/
  def test3(DF:DataFrame): Unit ={
    //显示特定的条数 m默认列最多显示20个字符（false 关闭该功能）
    DF.select("args","client_ip","uid").show(4,false)
  }

  def test4(DF:DataFrame): Unit ={
    DF.groupBy("args").count().show()
  }

  def collection(DF:DataFrame): Unit ={
    println(DF.collect().length)
    println(DF.collectAsList().size())
  }

  def descDetail(DF:DataFrame): Unit ={
    DF.describe("args").show()
  }


  def takeRecord(DF:DataFrame): Unit ={
    println(DF.first())
    println( DF.head(3).length)
  }

  def whereDemo(DF:DataFrame): Unit ={
    /*自带了select语句的where   你可以不选择args 只把它当做条件来使用*/
    DF.select("args","client_ip").where("args='king'").show()
    DF.select("args","client_ip").filter("args='king'").show()
  }

  def dropDuplicates(DF:DataFrame): Unit ={
    DF.dropDuplicates("args").show(3,false)

  }

  def castDemo(DF:DataFrame): Unit ={
    DF.select(DF("status").cast("bigint")).show()
    /*好像只能一列一列的将数据表示了*/
    DF.select(DF("*"),DF("status").cast("bigint")).show()

  }

}
