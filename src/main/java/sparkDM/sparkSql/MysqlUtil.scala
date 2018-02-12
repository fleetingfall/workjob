package sparkDM.sparkSql

import java.util.Properties

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object MysqlUtil {


  var mysqlConf=new Properties()
  mysqlConf.setProperty("driver", "com.mysql.cj.jdbc.Driver")
  mysqlConf.setProperty("user", "root")
  mysqlConf.setProperty("password", "www1234")
  mysqlConf.setProperty("verifyServerCertificate","false")
  /*采取了工厂设计模式  后面那个方法的意义所在*/
  val sparkSession:SparkSession=SparkSession.builder().appName("Base Demo").master("local[2]").getOrCreate()
  /*SparkSession 的内部， 包含了SparkContext， SharedState，SessionState */
  /*研究一下那些方法可以读成DataFrame  .csv() 方法可以*/
  val df:DataFrame=sparkSession.read.json("D:\\workingspace\\Code\\workjob\\src\\main\\resources\\json.txt").toDF("args","client_ip","host","is_blocked","status","uuid")
  df.show()
  /*获取元数据的方法*/
  df.printSchema()
  df.createOrReplaceTempView("kingcall")
  val result:DataFrame=sparkSession.sql("select uuid from kingcall")
  result.show()
  result.write.mode(SaveMode.Append).jdbc("jdbc:mysql://localhost:3306/kingcall?&serverTimezone=UTC&characterEncoding=utf8", "test", mysqlConf)
  sparkSession.stop()
}
