package util

import java.util.concurrent.Executors

import com.alibaba.fastjson.JSONObject
import org.apache.http.client.methods.HttpPost
import org.apache.http.entity.StringEntity
import org.apache.http.impl.client.HttpClientBuilder

/**
  * 发送微信报警通知工具类
  */

object WechatUtils extends Logging {
  val taskPool = Executors.newFixedThreadPool(10)

  def sendWechatMsg(jobName: String, msg: String) = {
    taskPool.execute(new Runnable {
      override def run() = {
        val httpClient = HttpClientBuilder.create.build
        try {
          val url = "http://10.52.7.39:10010/send_error"
          val post = new HttpPost(url)
          val json = new JSONObject()
          json.put("name", jobName)
          json.put("appId", "1000002")
          json.put("msg", msg)
          val entity = new StringEntity(json.toString, "utf-8")
          post.setEntity(entity)
          httpClient.execute(post)
          //          val response = httpClient.execute(post)
          //          val result = EntityUtils.toString(response.getEntity)
          //          logWarning("call back response entity : [ " + result + " ]")
        } catch {
          case e: Exception =>
            e.printStackTrace()
            logError("send wechat notify failed !")
        } finally {
          httpClient.close()
        }
      }
    })
  }
}
