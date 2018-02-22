package constant

/**
  * Created by Andy on 2016/9/30 0030.
  */
object ConfigFiles extends Enumeration {
  type ConfigFiles = Value
  val REDIS_LOCAL = Value("redis.local.properties")
  val REDIS_REMOTE = Value("redis.remote.properties")
  val COMMON_LOCAL = Value("common.local.properties")
  val COMMON_REMOTE = Value("common.remote.properties")
}
