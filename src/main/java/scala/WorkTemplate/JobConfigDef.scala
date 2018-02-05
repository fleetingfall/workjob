package scala.WorkTemplate

/**
  * 任务常用参数定义
  */
object JobConfigDef extends Enumeration {

  val ZOOKEEPER_LIST = Value("zk.list")
  val ZOOKEEPER_PATH = Value("zk.path")
  val GROUP_ID = Value("group.id")

  val TOPIC_NAME = Value("topic.name")
  val JOB_NAME = Value("job.name")
  val JOB_DURATION = Value("job.duration")

  val KAFKA_BROKER_LIST = Value("kafka.broker.list")
  val HBASE_ZOOKEEPER_QUORUM = Value("hbase.zookeeper.quorum")
  val ENABLE_REDIS = Value("enable.redis")
  val ENABLE_HIVE = Value("enable.hive")
  //jdbc
  val MYSQL_USER = Value("mysql.user")
  val JDBC_URL = Value("jdbc.url")
  val MYSQL_PWD = Value("mysql.pwd")
  val MYSQL_DRIVER = Value("mysql.driver")

  val TMP_TABLE=Value("tmp.table")
  val FIELDS=Value("fields")
  val SQL=Value("sql")

  val SAVE_PATH = Value("save.path")

  implicit def anyToString(any: Any): String = any.toString
}
