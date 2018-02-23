package util

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.regex.Pattern
import java.util.{Calendar, Date}

import org.joda.time.DateTime

/**
  * Created by Andy on 2016/12/7 0007.
  */
object DateUtils {
  val MINUTE_FORMAT = "yyyyMMddHHmm"
  val _MINUTE_FORMAT = "yyyy-MM-dd HH:mm"
  val HOUR_FORMAT = "yyyyMMddHH"
  val _HOUR_FORMAT = "yyyy-MM-dd HH"
  val SECOND_FORMAT = "yyyyMMddHHmmss"
  val _SECOND_FORMAT = "yyyy-MM-dd HH:mm:ss"
  val DAY_FORMAT = "yyyyMMdd"
  val _DAY_FORMAT = "yyyy-MM-dd"

  def isNumeric(str: String) = {
    val p = Pattern.compile("[0-9]*")
    p.matcher(str).matches()
  }

  def parseUnixTimeToDateStr(time: String, format: String = "yyyyMMdd") = {
    val ts = new Timestamp(s"${time}000".toLong)
    formatDate(ts, 0, format)
  }

  def parseUnixTimeToDate(ts: String, format: String = "yyyyMMddHHmmss") = {
    val r1 = parseUnixTimeToDateStr(ts, format)
    formatTimeToDate(r1, format)
  }

  def hoursAgo(ts: String, hour: Int = 1, format: String = "yyyyMMddHHmmss") = {
    val date = parseUnixTimeToDate(ts, format)
    val dateTime = new DateTime(date).minusHours(hour)
    dateTime.toDate
  }

  def formatDate(date: Date, amount: Int = 1, format: String = "yyyy-MM-dd") = {
    val cal = Calendar.getInstance()
    cal.setTime(date)
    cal.add(Calendar.DATE, amount)
    val df = new SimpleDateFormat(format)
    df.format(cal.getTime)
  }


  /**
    * 获取当前时间的时间戳(默认秒)
    *
    * @return 时间戳（秒）long
    */
  def getCurrentTimestamp(unit: String = "s"): Long = {
    val sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val date: String = sdf.format(new Date())
    val dt2 = sdf.parse(date)
    unit match {
      case "ms" => dt2.getTime
      case "s" => dt2.getTime / 1000
    }
  }

  def getCurrentTimestamp: Long = System.currentTimeMillis() / 1000

  def getDayBeginTimestamp: Long = {
    val cal = Calendar.getInstance
    cal.setTime(new Date)
    cal.set(Calendar.HOUR_OF_DAY, cal.getActualMinimum(Calendar.HOUR_OF_DAY))
    cal.set(Calendar.MINUTE, cal.getActualMinimum(Calendar.MINUTE))
    cal.set(Calendar.SECOND, cal.getActualMinimum(Calendar.SECOND))
    cal.getTime.getTime / 1000
  }

  def getDayEndTimestamp: Long = {
    val cal = Calendar.getInstance
    cal.setTime(new Date)
    cal.add(Calendar.DATE, 1)
    cal.set(Calendar.HOUR_OF_DAY, cal.getActualMinimum(Calendar.HOUR_OF_DAY))
    cal.set(Calendar.MINUTE, cal.getActualMinimum(Calendar.MINUTE))
    cal.set(Calendar.SECOND, cal.getActualMinimum(Calendar.SECOND))
    cal.getTime.getTime / 1000
  }

  /**
    * 获取当前日期
    *
    * @return 返回日期字符串
    */
  def getCurrentDate = formatDate(new Date(), 0)

  def getSevenDayAgo = formatDate(new Date(), -7)

  def getPassDay = formatDate(new Date(), -1)

  /**
    * 获取指定时间的时间戳(秒)
    *
    * @param dt     时间字符串
    * @param format 时间格式，默认为：yyyy-dd-MM HH:mm:ss
    * @return
    */
  def getTimestamp(dt: String, format: String = "yyyy-MM-dd HH:mm:ss"): Long = {
    val fm = new SimpleDateFormat(format)
    val dt2 = fm.parse(dt)
    dt2.getTime / 1000
  }


  /**
    * 将字符串转换成指定的日期格式
    *
    * @param datetime
    * @param format
    * @return
    */
  def formatDateTime(datetime: String, format: String = "yyyy-MM-dd HH:mm:ss") = {
    val sdf: SimpleDateFormat = new SimpleDateFormat(format)
    val date = sdf.parse(datetime)
    sdf.format(date)
  }

  def formatTimeToDate(datetime: String, format: String = "yyyy-MM-dd HH:mm:ss") = {
    val sdf: SimpleDateFormat = new SimpleDateFormat(format)
    sdf.parse(datetime)
  }

  /**
    * format from string time
    *
    * @return
    */
  def formatDateFromString(dateTime: String,
                           sourceFormat: String = "yyyy-MM-dd HH:mm:ss",
                           toFormat: String = "yyyy-MM-dd") = {
    val tmpDate = formatTimeToDate(dateTime, sourceFormat)
    formatDate(tmpDate, 0, toFormat)
  }

  def getMinutesAgo(dif: Int = -5, format: String = MINUTE_FORMAT) = {
    val cal = Calendar.getInstance()
    cal.setTime(new Date())
    cal.add(Calendar.MINUTE, dif)
    val df = new SimpleDateFormat(format)
    df.format(cal.getTime)
  }

  def minuteAgoTimeStamp(dif: Int = -5, format: String = MINUTE_FORMAT) = {
    val cal = Calendar.getInstance()
    cal.setTime(new Date())
    cal.add(Calendar.MINUTE, dif)
    cal.getTimeInMillis / 1000
  }

  def getHoursAgo(dif: Int = -1, format: String = HOUR_FORMAT) = {
    val cal = Calendar.getInstance()
    cal.setTime(new Date())
    cal.add(Calendar.HOUR, dif)
    val df = new SimpleDateFormat(format)
    df.format(cal.getTime)
  }

  /**
    * 从date时间字符串中取分钟片段
    */
  def fetchMinute(timeStr: String) = {
    timeStr.substring(11, 16)
  }
}
