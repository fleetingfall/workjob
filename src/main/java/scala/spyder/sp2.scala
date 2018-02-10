package scala.spyder

import org.jsoup.Jsoup
import org.jsoup.nodes.Document
import scala.collection.JavaConversions._
import scala.util.{Failure, Success, Try}

object sp2 {
  // Url源
  val url = "http://ke.jikexueyuan.com/xilie/?page=%d"
  // 存储路径
  val BASE_PATH = "D:\\workingspace\\spider\\scala2\\"

  def sleep(i: Long) = Thread.sleep(i)
  // 这里设置你的vip账户的cookies信息，用F12你应该懂的
  val cookies = new java.util.HashMap[String,String]
  cookies.put("uname", "2388054826@qq.com")
  cookies.put("authcode", "lwq123456")
  implicit class StringImprovement(val s : String){
    // 删除文件名中的非法字符
    def stripIllegalChar = s.replace("???","").replaceAll("\\\\|/|:|\\*|\\?|<|>|\\||\"", "")
  }
  def fCrawl(url: String): Document =
    Try(Jsoup.connect(url).timeout(0).cookies(cookies).get()) match {
      case Failure(e) => println(e.getMessage); sleep(10000); fCrawl(url)
      case Success(d) => d
    }

  def fDownload(path: String, file: String, url: String): Unit =
    Try(JavaHelper.download(path, file, url)) match {
      case Failure(e) => println(e.getMessage); sleep(10000); JavaHelper.download(path, file, url)
      case Success(_) =>
    }

  def main(args: Array[String]): Unit = {
    for( i <- 1 to 5;
         e1 <- fCrawl(url.format(i)).select("div.lesson-card");
         path1 = e1.select("h2").text.stripIllegalChar;
         (e2,j)  <- fCrawl(e1.select("div.text a").attr("href")).select("div.lesson-item").zipWithIndex;
         path2 = j + 1 + "_" +e2.select("dt.title").text.stripIllegalChar;
         e3 <- fCrawl(e2.select("a").attr("href")).select("dl.lessonvideo-list a")){
      val file = e3.text.stripIllegalChar+".mp4"
      val path = BASE_PATH + "/" + path1 + "/" + path2
      if (!(new java.io.File(path+"/"+file)).exists){
        fDownload(path, file, fCrawl(e3.attr("href")).select("source").attr("src"))
        println(s"$path\t$file\t下载成功！")
      }else{
        println(s"$path\t$file\t已下载！")
      }
    }
  }

}
