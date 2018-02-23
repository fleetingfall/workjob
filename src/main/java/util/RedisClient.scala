package util

import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}
import constant.RedisDef._


class RedisClient(host: String, port: String) extends Serializable {

  val write = getRedisPool(host, port.toInt)

  def getClient: Jedis = {
    write.getResource
  }

  def getRedisPool(host: String, port: Int) = {
    val jedisConfig: JedisPoolConfig = initRedisConf
    new JedisPool(jedisConfig, host, port, 600000)
  }

  def initRedisConf = {
    val conf = new JedisPoolConfig()
    conf.setMaxTotal(1000)
    conf.setMaxIdle(1000)
    conf.setMaxWaitMillis(6000)
    conf.setTestOnBorrow(true)
    conf
  }

}

/**
  * redis client
  */
object RedisClient extends Serializable with ConfigUtil {

  private val REDIS_PORT_1 = redisConfig(REDIS_PORT).toInt
  private val REDIS_PORT_2 = redisConfig(REDIS2_PORT).toInt
  private val idfaPort = redisConfig(IDFA_PORT).toInt

  val write = getRedisPool(redisConfig(WRITE_HOST), REDIS_PORT_1)
  val read = getRedisPool(redisConfig(READ_HOST), REDIS_PORT_1)
  val write2 = getRedisPool(redisConfig(WRITE2_HOST), REDIS_PORT_2)
  val read2 = getRedisPool(redisConfig(READ2_HOST), REDIS_PORT_2)

  val idfaWrite = getRedisPool(redisConfig(IDFA_WRITE_HOST), idfaPort)
  val idfaRead = getRedisPool(redisConfig(IDFA_READ_HOST), idfaPort)

  lazy val hook = new Thread {
    override def run() = {
      println("Execute hook thread: " + this)
      write.destroy()
      //      bRedisPool.destroy()
    }
  }
  sys.addShutdownHook(hook.run())

  def getWriteClient: Jedis = {
    write.getResource
  }

  def getReadClient = {
    read.getResource
  }

  def getWrite2Client: Jedis = {
    write2.getResource
  }

  def getRead2Client = {
    read2.getResource
  }

  def getIdfaWrite: Jedis = {
    idfaWrite.getResource
  }

  def getIdfaRead: Jedis = {
    idfaRead.getResource
  }

  def getRedisPool(host: String, port: Int) = {
    val jedisConfig: JedisPoolConfig = initRedisConf
    new JedisPool(jedisConfig, host, port, 600000)
  }


  def initRedisConf = {
    val conf = new JedisPoolConfig()
    conf.setMaxTotal(5000)
    conf.setMaxIdle(1000)
    conf.setMaxWaitMillis(6000)
    conf.setTestOnBorrow(true)
    conf
  }
}
