package scala.firstwork

import com.alibaba.fastjson.JSON
import kafka.serializer.StringDecoder
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka.KafkaUtils

import scala.WorkTemplate.Job
import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import scala.util.DateUtil

case class Bean(client_ip:String,is_blocked:String,args:String,status:String,uid:String,host:String,request_timestamp:String)

/**
  * 采取Spark-Streaming和Kafka直连的方式,但是不知道为什么要等很久才可以获取到数据（相比另一种对接的方式），
  */
object CombineStreamingSQL extends Job{
  val sparkSession:SparkSession=SparkSession.builder().appName("Base Demo").master("local[2]").getOrCreate()
  import sparkSession.sqlContext.implicits._

  def initwork(): Unit ={
    configs=initJobConf("SaveResty-10.properties")
    /*由于模拟时间产生的时间戳是13位，在转换时出现问题，所以除了1000后再传递给了 时间函数 */
    val sql=configs("sql").replace("@", "from_unixtime").replace("request_timestamp","request_timestamp/1000").format(configs("fields"))
    configs += ("sql" -> sql)

  }
  def test(): Unit ={

  }

  def main(args: Array[String]): Unit = {
    initwork()

    sparkSession.udf.register("domaindeal",domaindeal _)
    val scc=new StreamingContext(sparkSession.sparkContext,Seconds(2))
    val kafkaParams = Map(
      "metadata.broker.list" -> "master:9092"
    )
    val topics = Set("ktp")
    /*查没有过期的用法是什么*/
    val inputrdd=KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](scc,kafkaParams,topics )
    val SQLContext=sparkSession.sqlContext
    import SQLContext.implicits._
    /*发送过来的数据是 k-v 形式的      你是将RDD转换成DF的，而不是输入流          toDF()方法的参数是重新起列的名字吗*/
    println("准备接受数据了")
    /*这个列表是将来要合成一列的所有列*/
    val fieldList:List[String]="client_ip,is_blocked,args,status,uid,host,request_timestamp".split(",").toList
    inputrdd.foreachRDD(rdd=>{
      if(!rdd.isEmpty()){
        dealMethod2(sparkSession,parseRDDByStruct(sparkSession,rdd))
      }
    }
    )
    scc.start()
    scc.awaitTermination()
  }

  /**
    * 隐式转换函数  用来创建表结构  被parseRDDByStruct 函数调用
    * @param fieldsStr
    * @return
    */

  implicit def defineSchema(fieldsStr:String):StructType={
    println("隐式转换函数被调用")
    StructType(
      fieldsStr.split(",").map(fieldName ⇒ StructField(fieldName, StringType, true))
    )
  }

  /**
    * StructType  类型的转换 舍弃一些不必要的列
    * @param sparkSession
    * @param rdd
    * @return
    */

  def parseRDDByStruct(sparkSession: SparkSession,rdd:RDD[(String,String)]): DataFrame ={
    val read=sparkSession.read
    read.schema(configs("fields"))  // 这方法每次都回去调用隐式转换函数
    val DF=read.json(rdd.map(x=>x._2))
    DF.createOrReplaceTempView("tmps")
    val DF2=sparkSession.sql(configs("sql2"))
    DF2
  }

  /**
    * 从流中直接处理json 格式的RDD
    * @param rdd
    */
  def parseRDDByJson(sparkSession: SparkSession,rdd:RDD[(String,String)]): DataFrame ={
    val DF:DataFrame=sparkSession.read.json(rdd.map(x=>x._2))
    DF.show()
    DF
  }

  /**
    * 通过隐式转换解析的方式来处理
    * @param rdd
    * @return
    */
  def parseRDD(rdd:RDD[(String,String)]): DataFrame ={
    rdd.map(x => {
      var tmpobj = JSON.parseObject(x._2)
      /*其实在这里写一个工具类，将简单对象的JSON字符串----->对象        */
      Bean(tmpobj.get("client_ip").toString, tmpobj.get("is_blocked").toString, tmpobj.get("args").toString,
        tmpobj.get("status").toString, tmpobj.get("uid").toString, tmpobj.get("host").toString, tmpobj.get("request_timestamp").toString.substring(0,10))
    }).toDF()
  }

  //第一种处理方式，需要在textFormat 方法中完成 不借助SQL,通过函数来构造所需的分区 当然它的结果是可以被第二种存储方式直接存储的
  def dealMethod1(DF:DataFrame,fieldList:List[String]): Unit ={
    val DF1=DF.mapPartitions(textFormat(_,fieldList)).toDF("year", "month", "day","Hour","Minutes","domain_host","record")
    saveAsStreamingText(DF1)
  }
  //第二种种处理方式  借助SQL来构造所需的分区，但是可能会需要一个函数，否则SQL会很长(可不可以借助 in 来解决 not in的全部是others)
  def dealMethod2(sparkSession: SparkSession,DF:DataFrame): Unit ={
    DF.createOrReplaceTempView(configs("tmp.table"))
    val DF2:DataFrame=sparkSession.sql(
      configs("sql")
    )
    saveStreamingAsOrc(DF2)

  }

  /*
  * 不存在单列问题
  * */
  def saveStreamingAsOrc(df: DataFrame,
                         saveMode: String = "append"): Unit = {
    df.repartition(1).write.mode(saveMode)
      .partitionBy(configs("partition.format").split(","):_*)
      .orc(configs("save.path.orc"))
  }


  /*
  * 验证DF的直接存储  忘记了Hive的分区原则了吗，就是某一列啊
  * 由于 DataFrame 是一张表，所以在存储的时候每一行都有个分隔符，option就是字段之间的分割符号
  * 存在一个问题：除过分区列，只能有一列，也就是除过分区列，其他列要合并成一列
  * */
  def saveAsStreamingText(df: DataFrame): Unit = {
    df.repartition(1)
      .write.mode("append")
      .partitionBy("year", "month", "day","Hour","Minutes","domain_host")
      .option("delimiter", "|")
      .text("C:\\Users\\PLUSH80702\\Desktop\\receive")
  }

  /**
    * 数据预处理的一个方法    最终返回的结果格式   p1,p2,p3......record (p就是partion)
    * @param iterator     实际上就是一个DF
    * @param fieldsList   要提取的字段（不是整个DF中的信息你都是需要的）
    * @return Iterator[(年, 月, 日, 小时,分钟,host,record)]
    */
  def textFormat(iterator: Iterator[Row], fieldsList: List[String]): Iterator[(String, String, String, String,String,String,String)] = {
    val res = ArrayBuffer.empty[(String, String, String, String,String,String,String)]
    iterator.foreach(row => {
      val records = ListBuffer[String]()
      val valueMap = row.getValuesMap(fieldsList)
      /*形成一个record记录*/
      fieldsList.foreach { field =>
        try {
          val value = valueMap.getOrElse(field, "null")
          value match {
            case "" => records.+=(null)
            case _ => records.+=(value)
          }
        } catch {
          case e: Exception =>
            e.printStackTrace()
            val msg = s"=text format error= record -> [ field:$field , values :${valueMap.mkString("|")} ,error msg:${e.getMessage} ]"
        }
      }
      res.+=((DateUtil.getDateNowByArray()(0), DateUtil.getDateNowByArray()(1),DateUtil.getDateNowByArray()(2),DateUtil.getDateNowByArray()(3),
        DateUtil.getDateNowByArray()(4),domaindeal(row.getString(5)),records.mkString(",")))
    })
    res.iterator
  }

  /**
    * 关于domain_host分区列处理  存在则直接返回  不存在则归于其他分区,在两个地方使用到了，一个是作为普通的函数被调用，另一个是作为Spqrk-SQL的udf
    * @param host
    * @return
    */
  def domaindeal(host:String): String ={
    val domain=List("api.longzhu.com","api.plu.cn","betapi.longzhu.com", "configapi.longzhu.com", "event-api.longzhu.com")
    if(domain.contains(host))
      host
    else
      "others"
  }
}
