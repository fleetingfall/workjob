package sparkDM.sparkSql

import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

/*研究一下为什么元祖也转换不成 DF*/

object HiveFunctionDemo {
  def main(args: Array[String]): Unit = {
    val ss:SparkSession=SparkSession.builder().master("local[2]").appName("kingcall").enableHiveSupport().getOrCreate()
    RealWar(ss)
  }
  /*是所有函数的基础  获取DataFrame*/
  def getDF(ss:SparkSession): DataFrame ={
    val data = Seq[(String,String)](
      ("{\"userid\":\"1\",\"action\":\"0#222\"}","20180131"),
      ("{\"userid\":\"1\",\"action\":\"1#223\"}","20180131"),
      ("{\"userid\":\"1\",\"action\":\"2#224\"}","20180131"),
      ("{\"userid\":\"1\",\"action\":\"1#225\"}","20180131"),
      ("{\"userid\":\"1\",\"action\":\"2#225\"}","20180131"),
      ("{\"userid\":\"1\",\"action\":\"0#226\"}","20180131"),
      ("{\"userid\":\"1\",\"action\":\"1#227\"}","20180131"),
      ("{\"userid\":\"1\",\"action\":\"2#228\"}","20180131"),
      ("{\"userid\":\"2\",\"action\":\"0#223\"}","20180131"),
      ("{\"userid\":\"2\",\"action\":\"1#224\"}","20180131"),
      ("{\"userid\":\"2\",\"action\":\"1#225\"}","20180131"),
      ("{\"userid\":\"2\",\"action\":\"2#228\"}","20180131")
    )
    val RddData=ss.sparkContext.parallelize(data)
    val RowRdd=RddData.map(x=>Row(x._1,x._2))
    val structType = StructType(Array(StructField("info", StringType, true),StructField("dt", StringType, true)))
    val Df:DataFrame=ss.createDataFrame(RowRdd,structType)
    Df
  }
  /*hive解析 JSON 格式的数据  以及字符串的分割*/
  def jsonDemo(ss:SparkSession): DataFrame ={
    val Df:DataFrame=getDF(ss)
    Df.createOrReplaceTempView("test")
    val DF:DataFrame=ss.sql(
      """
        |select
        |    get_json_object(info,'$.userid') as user_id,
        |    split(get_json_object(info,'$.action'),'#')[0] as act1,
        |    split(get_json_object(info,'$.action'),'#')[1] as act2
        |    from test
        |where dt=20180131
      """.stripMargin
    )
    DF.show()
    DF
  }

  /*有条件计数  case   ehen   then  else   end*/
  def countWithCase(ss:SparkSession): DataFrame ={
    val Df:DataFrame=getDF(ss)
    Df.createOrReplaceTempView("test")
    /*这里涉及到一个东西 count对某一列进行计数的时候不会计算为null值*/
    val DF:DataFrame=ss.sql(
      """
        |select
        |    get_json_object(info,'$.userid') as user_id,
        |    count(
        |         case
        |             when split(get_json_object(info,'$.action'),'#')[0]=='0' then 1
        |             else null
        |          end
        |          ) as session_count
        |from test
        |where dt=20180131
        |group by get_json_object(info,'$.userid')
      """.stripMargin
    )
    DF.show()
    DF
  }
   /*
   * 组内排序函数
   * */
  def PartionOrder(ss:SparkSession): DataFrame ={
    val Df:DataFrame=getDF(ss)
    Df.createOrReplaceTempView("test")

    /*简单介绍一下
    * row_Number() OVER (partition by 分组字段 ORDER BY 排序字段 排序方式asc/desc)
    * 想一想，看一看和group by的区别在哪里   会发现这个函数业务特点特别强
    )
    * */
    val DF1=ss.sql(
    """
        |select * ,row_number() over(partition by user_id order by action_ts asc) as tnfrom  from(
        |  select
        |     get_json_object(info,'$.userid') as user_id,
        |     split(get_json_object(info,'$.action'),'#')[0] as action_type,
        |     split(get_json_object(info,'$.action'),'#')[1] as action_ts
        |from test
        |where dt=20180131
        |) t
      """.stripMargin
    )
    /*
    * lag括号里理由两个参数，第一个是字段名，第二个是数量N，这里的意思是，取分组排序之后比该条记录序号小N的对应记录的指定字段的值，如果字段名为ts，N为1，就是取分组排序之后上一条记录的ts值。
    * 可能比较难理解，但是从数据看的话很容易理解
    * */
    val DF2=ss.sql(
      """
        |select * ,
        |    row_number() over(partition by user_id order by action_ts asc) as tn,
        |    lag(action_ts,1) over(partition by user_id order by action_ts asc) as prev_ts,
        |    lead(action_ts,1) over(partition by user_id order by action_ts asc) as next_tsfrom
        |from(
        |         select
        |             get_json_object(info,'$.userid') as user_id,
        |             split(get_json_object(info,'$.action'),'#')[0] as action_type,
        |             split(get_json_object(info,'$.action'),'#')[1] as action_ts
        |         from test
        |         where dt=20180131
        |)t
      """.stripMargin
    )
    DF2.show()
    DF1
  }

  def RealWar(ss:SparkSession): DataFrame ={
    val Df:DataFrame=getDF(ss)
    Df.createOrReplaceTempView("test")
    val DF=ss.sql(
      """
         |select
         |    t2.user_id,
         |    t2.action_type,
         |    t2.action_ts,
         |    t1.action_ts as session_ts from(
         |  select
         |    *,
         |    lead(action_ts,1) over(partition by user_id order by action_ts asc) as next_ts
         |  from(
         |       select
         |           get_json_object(info,' $.userid') as user_id,
         |           split(get_json_object(info,' $.action'),'#')[0] as action_type,
         |           split(get_json_object(info,' $.action'),'#')[1] as action_ts  from test
         |        where dt=20180131
         |              and split(get_json_object(info,' $.action'),'#')[0] == '0'
         |      ) as t
         | ) t1
         |inner join
         |(
         |  select
         |    get_json_object(info,' $.userid') as user_id,
         |    split(get_json_object(info,' $.action'),'#')[0] as action_type,
         |    split(get_json_object(info,' $.action'),'#')[1] as action_ts  from test
         |  where dt=20180131
         |  ) t2
         |on t1.user_id = t2.user_id
         |where
         |     ( t2.action_ts >= t1.action_ts    and    t2.action_ts < t1.next_ts)
         |  or ( t2.action_ts >= t1.action_ts    and    t1.next_ts is null)
     """.stripMargin
    )
    DF.show()
    DF
  }




}
