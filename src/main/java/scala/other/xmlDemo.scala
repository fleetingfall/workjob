package scala.other

object xmlDemo {

  def main(args: Array[String]): Unit = {
    readXml
  }

  def readXml(): Unit ={
    import scala.xml.XML
    val xml=XML.loadFile("src/main/resources/externalfile/Namenode.htm")
    xml.foreach(println(_))
  }

}
