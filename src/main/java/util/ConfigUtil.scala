package util

import java.net.InetAddress

import scala.collection.mutable
import scala.io.Source
import constant.IpEnum._
import constant.ConfigFiles._

/**
  * config 工具类
  */
trait ConfigUtil extends Logging {
  protected val redisConfig: mutable.Map[String, String] = {
    initConf(REDIS_LOCAL.toString, REDIS_REMOTE.toString)
  }

  private def initConf(localConf: String, remoteConf: String) = {
    val ip: String = getIP
    val configFile = ip match {
      case _ if ip.startsWith(DevelopEnv.toString) => LZUtil.readFile(localConf)
      case _ if ip.startsWith(TestEnv.toString) => LZUtil.readFile(remoteConf)
      case _ if ip.startsWith(RemoteEnv.toString) => LZUtil.readFile(remoteConf)
    }
    configFile
  }

  /**
    * 加载参数配置文件，根据ip判断出相应的运行环境
    */
  def initCommonConf: mutable.Map[String, String] = {
    initConf(COMMON_LOCAL.toString, COMMON_REMOTE.toString)
  }

  def getConfigByFile(file: String): Map[String, String] = {
    Source.fromInputStream(
      this.getClass.getClassLoader.getResource(file).openStream()
    ).getLines().filterNot(_.startsWith("#")).map(_.split("=")).map(x => (x(0), x(1))).toMap
  }


  /**
    * 获取本机ip地址
    *
    * @return
    */
  def getIP: String = {
    InetAddress.getLocalHost.getHostAddress
  }

}
