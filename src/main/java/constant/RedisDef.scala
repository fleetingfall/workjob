package constant

/**
  * redis 相关定义,
  */
object RedisDef extends Enumeration {
  type ConfigFiles = Value
  //渠道规则
  val CHANNEL_RULES = Value("channel:rules")
  val CHANNEL_RULES_NEW = Value("channel:rules:new")
  //用户渠道来源
  val SOURCE_CHANNEL = Value("source:channel")
  //赛事
  val GAME_USER = Value("tag:game:user")
  val LAST_USER_TRACK = Value("last:user:track")

  //任务异常详情
  val JOB_STATUS_ERROR = Value("job:status:error")
  val SPARK_JOB_LIST = Value("spark:job:list")

  //===============about redis ==============
  val WRITE_HOST = Value("redis.master.host")
  val READ_HOST = Value("redis.slave.host")
  val WRITE2_HOST = Value("redis.master2.host")
  val READ2_HOST = Value("redis.slave2.host")
  val REDIS2_PORT = Value("redis.port2")
  val REDIS_HOST = Value("redis.host")
  val REDIS_PORT = Value("redis.port")

  val IDFA_WRITE_HOST = Value("idfa.master.host")
  val IDFA_READ_HOST = Value("idfa.slave.host")
  val IDFA_PORT = Value("idfa.port")

  val REDIS_TIMEOUT = Value("redis.timeout")

  val STREAMING_OFFSET = Value("streaming.offset")

  implicit def anyToString(any: Any): String = any.toString
}
