package scala.firstwork

import java.io.{File, FileWriter}

object SaveToLocal {
  val localpath="C:\\Users\\PLUSH80702\\Desktop\\receive\\"
  def saveFile(context:String,name:String): Unit ={
    var filename=localpath+ name+".txt"
    println("文件的名字是:"+filename)
    val file:File=new File(filename)
    val fileWriter:FileWriter=new FileWriter(file,true)
    fileWriter.write(context)
    /*其实在这里可以将fileWriter缓存*/
    fileWriter.flush()
    fileWriter.close()
  }
}
