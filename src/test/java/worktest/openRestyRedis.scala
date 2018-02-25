package worktest

import com.alibaba.fastjson.JSON
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import util.{JedisDM, RedisClient}

object openRestyRedis {

  val sparkConf = new SparkConf().setAppName("SqlNetworkWordCount").setMaster("local[2]")
  sparkConf.set("spark.testing.memory", "2147480000")
  val sparkSession:SparkSession=SparkSession.builder().appName("Base Demo").master("local[2]").getOrCreate()
  val context=sparkSession.sparkContext

  def main(args: Array[String]): Unit = {

    writeToRedisByDF


  }

  /**
    * 将数据写入Redis 参数是RDD
    * @param cleanRdd
    */

  def writeToRedis(): Unit ={
    val rdd=context.textFile("src/main/resources/longzhuResty.txt")
    rdd.foreachPartition{
      part=>{
        part.foreach(x=>{
          val tmp0bj=JSON.parseObject(x)
          if(tmp0bj.containsKey("cookie") && tmp0bj.containsKey("uid")){
            val cookie=tmp0bj.get("cookie").toString
            val uid=tmp0bj.get("uid").toString
            if(cookie.contains("p1u_id") && !uid.equals("-1")){
              val stringArray=cookie.split(";").filter(_.contains("p1u_id="))
              if(stringArray.length==1){
                JedisDM.sadd("uid:"+uid,stringArray(0).replaceAll("p1u_id=",""))
              }
            }

          }

        })
      }
    }
  }

  /**
    * 将数据写入Redis参数是DataFrame
    * 发现一个问题spark读取 json文件成DF时候的字段顺序可能和json中的数据不一样
    * @param df
    */
  def writeToRedisByDF(): Unit ={
    val df:DataFrame=sparkSession.read.json("src/main/resources/longzhuResty.txt")
    df.createOrReplaceTempView("DF")
    val d2=sparkSession.sql("select uid,cookie from DF")
    d2.foreachPartition(part=>{
      part.foreach(row=>{
        val uid=row(0).toString
        val cookie=row(1).toString
        if(cookie.contains("p1u_id") && !uid.equals("-1")){
          val stringArray=cookie.split(";").filter(_.contains("p1u_id="))
          if(stringArray.length==1){
            JedisDM.sadd("uid:"+uid,stringArray(0).replaceAll("p1u_id=",""))
          }
        }
      })
    }
    )
  }



}
