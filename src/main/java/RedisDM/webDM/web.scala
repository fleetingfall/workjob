package RedisDM.webDM

import redis.clients.jedis.Jedis
import util.JedisDM

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.util.control._

case class USER(name:String,nickname:String,passwd:String)

object web {
  val loop = new Breaks;
  val TOCKEN:String="tocken:";
  val LOGIN:String="login:";
  val RECENT:String="recent:";
  val VIEWED:String="view";
  val ITEM:String="item:";
  val CART:String="cart:";
  //维持回话的个数
  val TOCKEN_CNT:Int=1000*10000
  /**
    * 检查令牌
    * @param tockenid  tocken:tockenid
    * @return
    * java的map对应的是scala的可变map 而且还需要隐式转换工具类
    */
  def check_token(tockenid:String ):scala.collection.mutable.Map[String,String] ={
    JedisDM.getHash(LOGIN+tockenid)
  }

  implicit def usertomap(uSER: USER): scala.collection.mutable.HashMap[String,String] ={

    new mutable.HashMap[String,String]{"name"->uSER.name
      "nickname"->uSER.nickname
      "passwd"->uSER.passwd
    }

  }
  /**
    * 更新tocken
    * @param tockenid
    * @param user
    * @param itemid
    */
  def update_tocken(tockenid:String,user:USER,itemid:String): Unit = {
    val time = System.currentTimeMillis()
    //如何将caseclass转换成map
    JedisDM.setHash(LOGIN + tockenid, usertomap(user)) //维护tocken和用户的关系
    JedisDM.zadd(RECENT, time, TOCKEN + tockenid) //记录tocken最后一次的登录时间  也就是session集合
    if (itemid != null && !itemid.eq("")) {
      JedisDM.zadd(VIEWED + tockenid, time, ITEM+itemid);
      //记住这个用法 默认从小到大排序  这个是从大到小  保留25个元素  虽然用法有不合理之处
      JedisDM.pool.getResource.zremrangeByRank(VIEWED + tockenid, 0, -26)
    }
  }

 /**
   *令牌清理函数：只保留最新的一百万个令牌
   * 每次最多清理100个
   */
  def clean_session(): Unit ={
    val jedis:Jedis=JedisDM.pool.getResource;
    while (true){
      val size=JedisDM.pool.getResource.zcard(RECENT)
      loop.breakable {
        if(size<=TOCKEN_CNT){
          Thread.sleep(1000)
          loop.break()
        }
        val end_index=Math.min(size-TOCKEN_CNT,100)
        val tockens=JedisDM.getzMembers(RECENT,0,end_index).toSet
        jedis.zrem(RECENT,tockens.toArray[String]:_*)
        JedisDM.deletekeys(getTockenid(LOGIN,tockens).toArray[String]:_*)
        JedisDM.deletekeys(getTockenid(VIEWED,tockens).toArray[String]:_*)
        JedisDM.deletekeys(getTockenid(CART,tockens).toArray[String]:_*)
      }

    }

  }

  /**
    * 一个辅助函数，实现tockenid的获取
    * @param prefis
    * @param set
    * @return
    */
  def getTockenid(prefis:String,set: Set[String]): Set[String] ={
    val tmpset=new scala.collection.mutable.HashSet[String]
    for(s<-set){
      tmpset.add(prefis+s.split(":")(1))
    }
    tmpset.toSet
  }

  /**
    * 添加或删除购物车
    * @param tockenid
    * @param itemid
    * @param cnt
    */
  def add_to_shoppingCart(tockenid:String,itemid:String,cnt:Int): Unit ={
    if(cnt<=0){
      JedisDM.delHashField(CART+tockenid,ITEM+itemid)
    }else{
      JedisDM.setHash(CART+tockenid,ITEM+itemid,cnt.toString)
    }
  }

  /**
    * 原著是网页页面缓存
    * 我这里该是什么缓存呢——商品缓存  要不要分类呢
    */
  def cache_request(): Unit ={

  }




}
