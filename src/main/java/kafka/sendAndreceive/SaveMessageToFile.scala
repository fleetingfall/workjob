package kafka.sendAndreceive

import java.io.{File, FileWriter}

object SaveMessageToFile extends Serializable {
  def savemessage(file: File,context:String): Unit ={
    println("存储函数被调用")
    val writer = new FileWriter(file,true)
    writer.write(context)
    writer.flush()
    writer.close()
  }

}
