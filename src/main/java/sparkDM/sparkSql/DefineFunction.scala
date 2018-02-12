package sparkDM.sparkSql

import com.alibaba.fastjson.JSON
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SQLContext, SparkSession}

/*
* 让spark-sql 支持hive 则spark-sql的自定义函数就是hive的自定函数
* */
object DefineFunction {

  def main(args:Array[String]){
    val ss:SparkSession=SparkSession.builder().appName("Base Demo").master("local[2]").getOrCreate()
    test2(ss)

  }

  /*自定义匿名函数*/
  def test1(ss:SparkSession): Unit ={
    // 构造模拟数据
    val names = Array("Leo", "Mary", "Jack", "Tom")
    val namesRDD = ss.sparkContext.parallelize(names, 5)

    val namesRowRDD = namesRDD.map{name:String => Row(name)}
    val structType = StructType(Array(StructField("name", StringType, true)))
    val namesDF = ss.createDataFrame(namesRowRDD, structType)

    // 注册一张names表
    namesDF.createOrReplaceTempView("names")

    // 定义和注册自定义函数
    // 定义函数：自己写匿名函数
    // 注册函数：SQLContext.udf.reqister()
    ss.udf.register("strLen", (str:String) => str.length())

    // 使用自定义函数
    ss.sql("select name, strLen(name) from names").show()

  }

  /*自定义非匿名函数  首先定义一个函数    然后定义一个测试函数调用*/
  def decodeJson(key:String,json:String="""{"args":"kingcall","client_ip":"024.441.325.633","host":"10.10.10.110"}"""):String={
    val tmpobj=JSON.parseObject(json)
    require(true==tmpobj.containsKey(key))
    tmpobj.getString(key)
  }
  def test2(ss:SparkSession): Unit ={
    val names = Array("""{"name":"kingcall","age":"20"}""",
      """{"name":"king","age":"23"}""","""{"name":"call","age":"25"}""", """{"name":"callking","age":"30"}""")
    val namesRDD = ss.sparkContext.parallelize(names)

    val namesRowRDD = namesRDD.map{name:String => Row(name)}
    val structType = StructType(Array(StructField("info", StringType, true)))
    val namesDF = ss.createDataFrame(namesRowRDD, structType)
    namesDF.createOrReplaceTempView("studentinfo")
    /*目前好像没有发现其他写法(也就是只能这样写)，下面代表的是默认参数*/
    ss.udf.register("parseJson", decodeJson _ )
    // 使用自定义函数
    ss.sql("select parseJson('name',info),parseJson('age',info) from studentinfo").show()
  }

}
