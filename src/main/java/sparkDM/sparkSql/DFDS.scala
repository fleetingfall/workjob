package sparkDM.sparkSql

import com.alibaba.fastjson.JSON
import kafka.serializer.StringDecoder
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

import sparkDM.firstwork.OriginalMessageBean

/*
* 实践告诉我case class 最好写在外面
* */
case class people(id:String,name:String,age:String)

object AboutDS{
  val sparkSession:SparkSession=SparkSession.builder().appName("Base Demo").master("local[2]").getOrCreate()
  import sparkSession.sqlContext.implicits
}

/**
  * DF其实也是支持一些RDD的操作
  */
object toDF {
  val sparkSession:SparkSession=SparkSession.builder().appName("Base Demo").master("local[2]").getOrCreate()

  def main(args: Array[String]): Unit = {
    jsonDF(sparkSession)
  }

  def StructTypeMethod(sparkSession:SparkSession): Unit ={
    /*
    *  字段比较少的时候可以采用下面的方式书写
    *  val testSchema = StructType(Array(StructField("ID", StringType, true), StructField("Name", StringType, true), StructField("District", StringType, true)))
    * */
    val schemaString = "id name age"
    val schema = StructType(schemaString.split(" ").map(fieldName ⇒ StructField(fieldName, StringType, true)))
    /* 虽然有这样一个函数，sparkSession.read.textFile()，但是你不要用，会出错误*/
    val rowRDD = sparkSession.sparkContext.textFile("src/main/resources/people.txt").map(x => x.split(",")).map( p => Row(p(0), p(1), p(2)))
    val testDF = sparkSession.createDataFrame(rowRDD, schema)
    testDF.createOrReplaceTempView("test")
    val resultDF=sparkSession.sql("SELECT * FROM test")
    resultDF.show()
  }

  /*：隐式转换会将含有 case 对象的 RDD 转换为 DataFrame */
  def casemethod(sparkSession:SparkSession): Unit ={
    import sparkSession.sqlContext.implicits._
    val rowRDD = sparkSession.sparkContext.textFile("src/main/resources/people.txt")
    val caseRDD=rowRDD.map(x=>x.split(",")).map(x=>people(x(0),x(1),x(2))).toDF().show()
  }
  /*
  * 将本地的json读成DF
  * */
  def jsonDF(sparkSession:SparkSession): Unit ={
    val DF=sparkSession.read.json("src/main/resources/json.txt").toDF()
    DF.show()

  }

  /*
  * 直接从流中读取成DF
  * */
  def jsonToDF(sparkSession:SparkSession): Unit ={
    println("接收数据开始")
    import sparkSession.sqlContext.implicits._
    val scc=new StreamingContext(sparkSession.sparkContext,Seconds(2))
    val kafkaParams = Map(
      "metadata.broker.list" -> "master:9092"
    )
    val topics = Set("longzhuresty")
    /*查没有过期的用法是什么*/
    val inputrdd=KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](scc,kafkaParams,topics )
    inputrdd.foreachRDD(rdd=>{
      if (!rdd.isEmpty()){
        sparkSession.read.json(rdd.map(x=>x._2)).show()
      }
    })
    scc.start()
    scc.awaitTermination()
  }


  /*
  * 新的问题是如何将java对象的DF转换成DF
  * */
  def jsonObjToDF2(sparkSession:SparkSession): Unit ={
    val s="""{"args":"kingcall","client_ip":"847.478.245.838","host":"110.110.110.110","is_blocked":"1","status":"200","uid":"ca7e38a74bbd4f478a70005047bf6ccc"}"""
    val p:OriginalMessageBean=JSON.parseObject(s).toJavaObject(classOf[OriginalMessageBean])
    println(p)
  }


}
