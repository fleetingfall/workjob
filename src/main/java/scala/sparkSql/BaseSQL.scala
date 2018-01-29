package scala.sparkSql

import java.text.SimpleDateFormat

import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.util.DateUtil

/**
  * 一些常用的SQL技巧
  *   1.  当语句比较长的时候，用 """   """ 三引号
  *   2.  当需要引入外部变量的时候   需要借助这样的语句  s"select '${time}' as time"
  *
  * 时间函数
  *   日期时间转换：
      unix_timestamp, from_unixtime, to_date, quarter, day, dayofyear, weekofyear, from_utc_timestamp, to_utc_timestamp
      从日期时间中提取字段：
      year, month, dayofmonth, hour, minute, second
      日期/时间计算：
      datediff, date_add, date_sub, add_months, last_day, next_day, months_between
      获取当前时间等：
      current_date, current_timestamp, trunc, date_format

  */
object BaseSQL {
  def main(args: Array[String]): Unit = {
    val sparksession=SparkSession.builder().appName("Test").master("local[2]").getOrCreate()
    WithTime(sparksession)

  }

  /**
    * case(unix_timestamp(t1.D504_11,'dd-MM-yyyy')
    * spark-SQL自带的时间处理函数
    */
  def WithTime(sparksession:SparkSession): Unit ={
    /*从时间获取对应的时间戳*/
    sparksession.sql("select unix_timestamp('2018-1-28' ,'DD-MM-yyyy') as time").show()
    /*从时间戳获取时间*/
    sparksession.sql("SELECT FROM_UNIXTIME( 1195488000, '%Y年%m月%d' )  as time").show()
    /*获取的是当前时间的时间戳字符串*/
    sparksession.sql("select unix_timestamp() as time").show()
    sparksession.sql("select unix_timestamp(new Date()) as time").show()
  }

  /*
  * 自定义函数
  *   1.比较简单功能的就自定义一个匿名函数
  *   2.比较复杂的就自定义单独写一个函数将其注册进来
  * */
  def defineFunction(sparksession:SparkSession): Unit ={
    val tmpstr="kingcall"
    sparksession.udf.register("strLen",(str:String)=>str+":"+str.length)
    sparksession.sql("select 'kingcall' as name ,strLen('kingcall')"
    ).show()
  }
  def getDate(time:String):Unit ={

    val now: Long = System.currentTimeMillis()

    var df: SimpleDateFormat = new SimpleDateFormat(time)
    df.format(now)
  }
  /*
  * 在SQL 语句中获取时间,由于spark-sql没有专门的时间函数，所以我们使用的时间都是scala或者java的
  * 也就是调用其函数或者方法 ,注册外部方法
  * */
  def getTime(sparksession:SparkSession): Unit ={
   /* sparksession.udf.register("getDate",getDate("yyyy"))
    sparksession.sql(
      s"""
        |select getDate() as time
        |
      """.stripMargin
    ).show()*/
  }

  /*
  * 在sql语句中引入外部变量,例如时间
  * */
  def test1(sparksession:SparkSession): Unit ={
    val time=DateUtil.getDateNow()
    val result:DataFrame=sparksession.sql(
      s"select '${time}' as time"
    )
    result.show()
    sparksession.stop()
  }

}