package scala.util

import java.io.{File, FileInputStream, FileOutputStream, IOException}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, FileSystem, FileUtil, Path}
import org.apache.hadoop.io.IOUtils
/*
* scala 操作HDFS的util
* 方案一：scala 类型的UTIL
* 方案二：JAVA  类型的UTIL
* */
object HdfsUtil {

  def main(args: Array[String]): Unit = {
    writeToHdfs("/lz/firstjob","kingcall.txt","Hello Kingcall")

  }


  /**
    * 获取文件系统
    * @return
    */
  def getFS(): FileSystem = {
    //System.setProperty("hadoop.home.dir", "D:\\04coding\\projects-bigData\\Hadoop\\hadoop-2.5.0") 研究一下这是windows下的方式吗
    System.setProperty("HADOOP_USER_NAME", "root")
    val conf = new Configuration()
    conf.setBoolean("dfs.support.append", true)
    conf.set("fs.defaultFS", "hdfs://master:9000/")
    /*当你连接不上的时候   Hadoop集群是否打开  防火墙是否关闭    端口是否写对*/
    conf.set("mapred.remote.os", "Linux")
    FileSystem.get(conf)
  }

  /**
    * 关闭FileSystem
    *
    * @param fileSystem
    */
  def closeFS(fileSystem: FileSystem) {
    if (fileSystem != null) {
      try {
        fileSystem.close()
      } catch {
        case e: IOException => e.printStackTrace()
      }
    }
  }

  /**
    * ls 获取文件详情
    * @param hdfsFilePath
    */
  def listFiles(hdfsFilePath: String): Unit = {
    val fileSystem = getFS()
    val fstats = fileSystem.listStatus(new Path(hdfsFilePath))
    try {

      for (fstat <- fstats) {
        if (fstat.isDirectory()) {
          println("directory")
        } else {
          println("file")
        }
        println("Permission:" + fstat.getPermission())
        println("Owner:" + fstat.getOwner())
        println("Group:" + fstat.getGroup())
        println("Size:" + fstat.getLen())
        println("Replication:" + fstat.getReplication())
        println("Block Size:" + fstat.getBlockSize())
        println("Name:" + fstat.getPath())
        println("#############################")
      }

    } catch {
      case e: IOException => e.printStackTrace()
    } finally {
      if (fileSystem != null) {
        try {
          fileSystem.close()
        } catch {
          case e: IOException => e.printStackTrace()
        }
      }
    }
  }

  def ls(fileSystem: FileSystem, path: String) = {
    println("list path:" + path)
    val fs = fileSystem.listStatus(new Path(path))
    val listPath = FileUtil.stat2Paths(fs)
    for (p <- listPath) {
      println(p)
    }
    println("----------------------------------------")
  }


  /**
    * 创建目录,支持递归创建的
    *
    * @param hdfsFilePath
    */
  def mkdir(hdfsFilePath: String) = {
    val fileSystem = getFS()

    try {
      val success = fileSystem.mkdirs(new Path(hdfsFilePath))
      if (success) {
        println("Create directory or file successfully")
      }
    } catch {
      case e: IllegalArgumentException => e.printStackTrace()
      case e: IOException => e.printStackTrace()
    } finally {
      this.closeFS(fileSystem)

    }
  }

  /**
    * 删除文件或目录
    *
    * @param hdfsFilePath
    * @param recursive 递归
    */
  def rm(hdfsFilePath: String, recursive: Boolean): Unit = {
    val fileSystem = this.getFS()
    val path = new Path(hdfsFilePath)
    try {
      if (fileSystem.exists(path)) {
        val success = fileSystem.delete(path, recursive)
        if (success) {
          System.out.println("delete successfully")
        }
      }

    } catch {
      case e: IllegalArgumentException => e.printStackTrace()
      case e: IOException => e.printStackTrace()
    } finally {
      this.closeFS(fileSystem)
    }
  }


  /**
    * 上传文件到HDFS
    * @param localPath
    * @param hdfspath
    */
  def loadToHdfs(localPath: String, hdfspath: String) {

    val inStream = new FileInputStream(
      new File(localPath)
    )
    val fileSystem = this.getFS()
    val writePath = new Path(hdfspath)
    val outStream = fileSystem.create(writePath)

    try {
      IOUtils.copyBytes(inStream, outStream, 4096, false)
    } catch {
      case e: IOException => e.printStackTrace()
    } finally {
      IOUtils.closeStream(inStream)
      IOUtils.closeStream(outStream)
    }

  }


  /**
    * 打印hdfs上的文件内容
    *
    * @param hdfsFilePath
    */
  def cat(hdfsFilePath: String) {

    val fileSystem = this.getFS()

    val readPath = new Path(hdfsFilePath)

    val inStream = fileSystem.open(readPath)

    try {
      IOUtils.copyBytes(inStream, System.out, 4096, false)
    } catch {
      case e: IOException => e.printStackTrace()
    } finally {
      IOUtils.closeStream(inStream)
    }
  }
  /**
    * 下载文件到本地
    *
    * @param localFilePath
    * @param hdfsFilePath
    */
  def loadToLocal(localFilePath: String, hdfsFilePath: String) {
    val fileSystem = this.getFS()
    try {
      val fsis = fileSystem.open(new Path(hdfsFilePath))
      val fos = new FileOutputStream(new File(localFilePath))
      IOUtils.copyBytes(fsis, fos, 1024)
    } catch {
      case e: IllegalArgumentException => e.printStackTrace()
      case e: IOException => e.printStackTrace()
    } finally {
      IOUtils.closeStream(fileSystem)
    }
  }

  /**
    * 向hdfs 上的文件写入内容
    * @param path    文件夹路径
    * @param name    文件名称
    * @param context 文件内容
    */
  def writeToHdfs(path:String,name:String,context:String): Unit ={
    val fileSystem = getFS()
    val outputStream=fileSystem.create(new Path(path+"/"+name),true)
    outputStream.writeUTF(context)
    outputStream.flush()
    outputStream.close()
  }

}
