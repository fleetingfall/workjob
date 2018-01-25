package scala.firstwork

import java.io.{File, FileWriter}
import java.util.Date
import scala.util.{DateUtil, HdfsUtil}


/*
*
* 将文件存储在HDFS上面
* 文件夹格式  年   月  日  小时  hostname
*
* */

object SaveToHDFS {

  val basepath="/lz/firstjob/"

  def getTimePath():String={
    val time=DateUtil.dateHourStr(new Date().getTime)
    val date=time._1
    val hour=time._2
    val path=date.split("-").mkString("/")+"/"+hour+"/"
    path
  }

  /**
    * 创建文件夹并且返回路径
    * @return 完整的文件路径
    */
  def createFolderAndGetPath():String={
    val path=basepath+getTimePath()
    HdfsUtil.mkdir(basepath)
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
    HdfsUtil.mkdir(completepath)
    val filename=completepath+name+".txt"
    println("文件的名字是:"+filename)
    /*问题是如何正真的完成每一条信息的保存，其实就是文件的追加*/
    HdfsUtil.writeToHdfs(completepath,name,context)
  }
}
