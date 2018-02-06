package scala

import java.io.File
import java.util.Date

import scala.io.Source
import scala.reflect.io.File

object test {
  def main(args: Array[String]): Unit = {
    val source=Source.fromFile("json.txt")
    source.getLines().count(x=>true)
  }



  def test(): Unit ={
    val configs=Map("sql"->"select @(request_timestamp,'yyyy-MM-dd') day ,@(request_timestamp,'HH') hour,domaindeal(host) as domain, %s from openresty",
      "fields"->"x_forwarded_for,is_blocked,args,status,cookie,request_timestamp,referer,host,method,scheme,response_time,user_agent,body,uri,client_ip,uid,uuid,hit"
    )
    val sql2 = configs("sql").replace("@", "from_unixtime").format(configs("fields"))
    println(sql2)
  }

}
