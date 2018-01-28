package kafka

import java.io.{File, FileWriter}
import java.util

import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}


object CombineStreaming {
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setAppName("TEst").setMaster("local[2]")
    val sc=new StreamingContext(conf,Seconds(2))
    val inputrdd=KafkaUtils.createStream(sc,"10.53.1.30:2181","kk",Map("kingcall"->1))
    val file=new File("C:\\Users\\kingc\\Desktop\\tmp\\1.txt")
/*    inputrdd.map(x=>
    {
      println(x._1)
      println(x._2)
      println("map 函数的处理结果")
    })*/
    var count=0
    inputrdd.foreachRDD(
      x=>{
        if (x.count()>0) {
          x.saveAsTextFile("C:\\Users\\kingc\\Desktop\\tmp\\"+count)
          count=count+1

         /*这个路径其实也是个文件夹，但它和流对象不一样的是它没有时间戳，所以多次输出的话会覆盖，需要去手动改变输出的文件夹
         * 而且保存的时候保存的是一个RDD对象，所以会包含很多信息，每个RDD对象就是一个文件夹，里面的每个partion就是一个单独的文件
         *
         * */

          x.foreach(y => {
            println("RDD:"+x+" 的内容主要包括")
            println(y._1)
            println(y._2)
            SaveMessageToFile.savemessage(file,y.toString())

            /*
            在编写Spark程序中，由于在map等算子内部使用了外部定义的变量和函数，从而引发Task未序列化问题
            fileWriter.write(y.toString())
            fileWriter.flush()
            */
          }
          )
        }
        }
    )
    /*
    * inputrdd.saveAsTextFiles("C:\\Users\\kingc\\Desktop\\tmp\\RDD",".txt")
    * 这个输出的路径有点特殊，RDD相当于文件名的一部分，而且也是按离散流的方式进行输出
    * 其实走到这里忽然就明白了如果数据量大，我就让其通过流式的方式直接进行输出，否则我应该等拿到数据后自己写scala脚本进行输出
    * */
    sc.start()
    sc.awaitTermination()
  }

}
