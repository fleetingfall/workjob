package util

import java.net.{URI, URLDecoder}
import java.text.DecimalFormat
import java.util.Random
import java.util.concurrent.Executors
import java.util.regex.Pattern

import org.apache.commons.codec.binary.Base64
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.http.client.config.RequestConfig
import org.apache.http.client.methods.HttpGet
import org.apache.http.impl.client.HttpClientBuilder
import org.apache.http.util.EntityUtils

import scala.collection.mutable
import scala.io.Source


object LZUtil extends Logging {

  //  val _log = LoggerFactory.getLogger(this.getClass)

  def readFile(fileName: String): mutable.Map[String, String] = {
    val prop = mutable.Map[String, String]()
    val kv = (s: String) => {
      val idx = s.indexOf("=")
      prop.+=(s.substring(0, idx) -> s.substring(idx + 1, s.length))
    }
    Source.fromInputStream(
      this.getClass.getClassLoader.getResource(fileName).openStream()
    ).getLines().filterNot(_.startsWith("#")).foreach(kv)
    prop
  }

  def readLine(fileName: String) = {
    Source.fromInputStream(
      this.getClass.getClassLoader.getResource(fileName).openStream()
    ).getLines().filterNot(_.startsWith("#"))
  }

  def getHostFromUri(uri: String): String = {
    if (uri == null) {
      throw new NullPointerException("uri can't be null!")
    }
    val uristr = new URI(uri)
    uristr.getHost
  }

  /**
    * 使用hdfs追加文件
    * val hadoopConf = sc.hadoopConfiguration
    * val hdfs = FileSystem.get(hadoopConf)
    * //创建hdfs 文件
    * val savePath = LZUtil.createHDFSFile(hdfs, s"$output/$curDate", "")
    * val fin = hdfs.append(savePath)
    * fin.writeBytes(" \n")
    *
    * @param hdfs
    * @param dirName
    * @param fileName
    * @return
    */
  def createHDFSFile(hdfs: FileSystem, dirName: String, fileName: String) = {
    val filePath = new Path(dirName)
    if (!hdfs.exists(filePath)) {
      hdfs.mkdirs(filePath)
    }
    val savePath = new Path(s"$dirName/$fileName")
    if (!hdfs.exists(savePath)) {
      hdfs.create(savePath).close()
    }
    savePath
  }

  def deCodeBase64(str: String) = {
    val base64 = new Base64()
    val dec = base64.decode(str)
    new String(dec, "UTF-8")
  }

  def parseUrlCode(url: String): String = {
    val utm = url.split("utm_sr=")
    if (utm.length >= 2) {
      //      logWarning(s"target url: $url")
      val tmp = utm(1)
      tmp.indexOf("&") match {
        case -1 => tmp.substring(0, tmp.length)
        case _ => tmp.substring(0, tmp.indexOf("&"))
      }
    } else {
      null
    }
  }


  def isContain(source: String, subItem: String) = {
    val pattern = "\\b" + subItem + "\\b"
    val p = Pattern.compile(pattern)
    val m = p.matcher(source)
    m.find()
  }

  def generateID(input: String): String = {
    if (input.isEmpty) throw new IllegalArgumentException("input str can't be null!")
    String.valueOf(input.hashCode & 0x7FFFFFFF)
  }

  def RandomKey(length: Int): String = {
    val source = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
    val random = new Random
    val buf = new StringBuilder
    var i: Int = 0
    while (i < length) {
      {
        buf.append(source.charAt(random.nextInt(62)))
      }
      {
        i += 1
        i - 1
      }
    }
    buf.toString
  }

  @transient
  val requestPool = Executors.newFixedThreadPool(100)

  /** *
    * 发送request请求
    *
    * @param url Url
    */
  def sendRequest(url: String) {
    //    requestPool.submit(
    //      new Runnable {
    //        override def run() = {
    val decodeUrl = URLDecoder.decode(url, "UTF-8")
    val clientBuilder = HttpClientBuilder.create
    val httpClient = clientBuilder.build
    try {
      val requestConfig = RequestConfig.custom()
        .setConnectTimeout(5000)
        .setConnectionRequestTimeout(1000)
        .setSocketTimeout(5000)
        .build()
      val httpGet = new HttpGet(decodeUrl)
      httpGet.setConfig(requestConfig)
      val response = httpClient.execute(httpGet)
      val result = EntityUtils.toString(response.getEntity)
      logWarning(s"""{"method":"sendRequest","url":"$decodeUrl","extra":"response:$result"}""")
      response.close()
    } catch {
      case e: Exception =>
        val error = s"sendRequest cause error with url : $url ,  error :${e.getMessage} "
        logError(s"""{"method":"sendRequest","url":"$decodeUrl","extra":"error:${e.getMessage}"}""")
        WechatUtils.sendWechatMsg("LZUtil:sendRequest", error)
        e.printStackTrace()
    } finally {
      httpClient.close()
    }
    //        }
    //      }
    //    )
    //      _log.warn(s"""{"method":"sendRequest","url":"$decodeUrl","extra":""}""")
  }


  def formatDoubleWithStr(d: String): String = {
    val tmp = d.toDouble
    val decimalFormat = new DecimalFormat("#.00")
    decimalFormat.format(tmp)
  }

  def formatDouble(d: Double): Double = {
    val decimalFormat = new DecimalFormat("#.0000")
    decimalFormat.format(d).toDouble
  }

  def replaceSymbol(s: String) = s.replaceAll("\\\r|\\\n|\\r|\\n|\\\\r|\\\\n|\r|\n|\\t|\\|", "")

  def filterConfig(prefix: String, m: mutable.Map[String, String]) = {
    m.filter(_._1.startsWith(prefix)).map {
      case (k, v) => k.replace(s"$prefix.", "") -> v
    }
  }
}