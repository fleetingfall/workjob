package scala.firstwork

import java.io.{File, FileWriter}
import java.util.Date

import org.apache.hadoop.io.Text
import org.apache.hadoop.mapred.TextOutputFormat
import org.apache.spark.streaming.dstream.ReceiverInputDStream

import scala.util.DateUtil

/*
*
* 这里有个问题  我每次保存一条记录的时候都要去判断路径或者创建文件夹，但在这里应该是每一次隔一小时调用一次应该写在 sparkstreaming中
* sparkstreaming将文件路径传给保存函数即可
*
* sparkstreaming保存方式————————————很多文件夹，文件夹下有很多文件
* RDD保存方式  特定的文件夹下面欧很多小文件会覆盖
*
* 尝试解决方案：streaming给一个路径（这个路径可以自己定义），RDD去保存
*
* saveAsHadoopDataset用于将RDD保存到除了HDFS的其他存储中，比如HBase。
* */

object SaveToLocal {
  /*基础路径，即在文件系统中的位置*/
  val basepath="C:/Users/PLUSH80702/Desktop/receive/"
  /*
  * 先根据时间构造文件路径，如果文件路径存在则直接写，如果路径文件不存在则创建文件夹
  * */

  def main(args: Array[String]): Unit = {
    println(getTimePath)
  }

  def getTimePath():String={
    val time=DateUtil.dateHourStr(new Date().getTime)
    val date=time._1
    val hour=time._2
    val path=date.split("-").mkString("/")+"/"+hour+"/"
    path
  }

  /**
    * 创建文件夹并且返回路径 路径精确到了小时
    * @return 完整的文件路径
    */
  def createFolderAndGetPath():String={
    println(" |----------------------------创建文件夹的方法被调用------------------------------|")
    val path=basepath+getTimePath()
    new File(path).mkdirs()
    path
}

  /**
    * 保存信息的方法，接收三个参数，还要根据ip(文件命)完成一次文件夹的创建
    * @param context 要保存的内容
    * @param name    文件的名字
    * @param path    完整的文件夹路径
    */
  def saveFile(path:String,context:String,name:String): Unit ={
    val completepath=path+name+"/"
    val folder:File=new File(completepath)
    folder.mkdirs()
    val filename=completepath+name+".txt"
    println("文件的名字是:"+filename)
    val file:File=new File(filename)
    val fileWriter:FileWriter=new FileWriter(file,true)
    fileWriter.write(context)
    /*其实在这里可以将fileWriter缓存*/
    fileWriter.flush()
    fileWriter.close()
  }

  /**
    * 直接保存流对象
    * @param inputStream 流对象
    */
  def saveByAtreaming(inputStream:ReceiverInputDStream[(String,String)]): Unit ={
    /*前缀-时间.后缀  那么只会保存在Executor所在机器的本地目录*/
    inputStream.saveAsTextFiles("C:\\Users\\PLUSH80702\\Desktop\\receive\\")
    /*支持在本地运行然后存储到hdfs上去*/
    inputStream.saveAsHadoopFiles("hdfs://master:9000/lz/firstjob","",classOf[Text],classOf[Text],classOf[TextOutputFormat[Text,Text]])
    /*保存结束之后应该合并文件夹里面的零散文件*/
  }
}
