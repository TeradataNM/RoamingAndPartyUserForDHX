package utils

import java.text.{ParseException, SimpleDateFormat}
import java.util.Date

import com.xiaoleilu.hutool.util.DateUtil._

/**
  * Created by lyndon on 2017/4/18.
  */
trait TimeFunc {

  def getCurrentTimeMillis: Long = System.currentTimeMillis()

  def timeMillsToDate(timeMills: Long, format: String = "yyyy-MM-dd"): String = {
    val sdf: SimpleDateFormat = try {
      new SimpleDateFormat(format)
    } catch {
      case e: IllegalArgumentException => throw e.fillInStackTrace()
      case e: NullPointerException => throw e.fillInStackTrace()
    }
    val date: String = sdf.format(new Date(timeMills))
    date
  }

  def strToDate(str: String, format: String = "yyyy-MM-dd"): Date = {
    val sdFormat = new SimpleDateFormat(format)
    var date: Date = null
    try {
      date = sdFormat.parse(str)
    } catch {
      case e: ParseException => e.printStackTrace()
    }
    date
  }

  def toDate(timeMills: Long): Date = {
    val date = new Date(timeMills)
    date
  }

  def toDate: Date = {
    new Date(getCurrentTimeMillis)
  }

  /**
    * 计算两个时间戳相差的时间
    *
    * @param start_Time
    * @param end_Time
    * @param diffType
    * @return
    */
  def getBetweenTime(start_Time: Long, end_Time: Long, diffType: String): Long = {
    val (diffField, toFormat) = diffType.toUpperCase match {
      case "DAY" => (DAY_MS, NORM_DATE_PATTERN) // 86400L * 1000
      case "HOUR" => (HOUR_MS, NORM_DATETIME_PATTERN) // 3600L * 1000
      case "WEEK" => (DAY_MS * 7, NORM_DATE_PATTERN)
      case "MIN" => (MINUTE_MS, NORM_DATETIME_MINUTE_PATTERN) // 60L * 1000
      case "SEC" => (SECOND_MS, NORM_DATETIME_PATTERN)
      case _ => (1L, NORM_DATETIME_MS_PATTERN)
    }
    val diffValue: Long = diff(parse(format(toDate(start_Time), toFormat)), parse(format(toDate(end_Time), toFormat)), diffField)
    math.abs(diffValue)
  }

  def getBetweenTime(start_Time: Date, end_Time: Date, diffType: String): Long = {
    val diffField = diffType.toUpperCase match {
      case "DAY" => DAY_MS // 86400L * 1000
      case "HOUR" => HOUR_MS // 3600L * 1000
      case "WEEK" => DAY_MS * 7
      case "MIN" => MINUTE_MS // 60L * 1000
      case "SEC" => SECOND_MS
      case _ => 1L
    }
    val diffValue: Long = diff(start_Time, end_Time, diffField)
    math.abs(diffValue)
  }

  def date2TimeStamp(dateStr: String, format: String): String = {
    try {
      val sdf = new SimpleDateFormat(format)
      return String.valueOf(sdf.parse(dateStr).getTime / 1000)
    } catch {
      case e: Exception =>
        e.printStackTrace()
    }
    ""
  }
}

object TimeFuncChk extends TimeFunc {
  def main(args: Array[String]) {
    println(toDate(System.currentTimeMillis()))
    println(getBetweenTime(1467713914611L, 1467792088616L, "DAY"))

    println(toDate(1492553560162L))
    println(toDate(1492553560168L))


    val time = "yyyyMMdd HH:mm:ss".substring(9, 14).split(":")
    println(time(0) + "..." + time(1))

    val today: Date = strToDate("2017-07-04")

    val nextDay: Date = strToDate("20170705", "yyyyMMdd")

    val nextNextDay: Date = strToDate("2017-6-30")

    val diff1 = getBetweenTime(today, nextDay, "DAY")

    val diff2 = getBetweenTime(today, nextNextDay, "DAY")

    println("diff1：" + diff1 + " diff2:" + diff2)
    val baseStationList = List("18372-106329771", "18372-106330027", "18372-106363297", "18372-81878549", "18372-79874337", "18372-43353", "18372-79750928", "18372-79874336", "18372-81820697", "18372-19412", "18372-81972512", "18372-79750924", "18372-81878550", "18372-79874335", "18372-81972511", "18372-19413", "18372-81972513", "18372-81820696", "18372-43352", "18372-79750926", "18372-81972514", "18372-43351", "18372-19411", "18372-81820694", "18372-81820693", "18372-79874338", "18372-81820695", "18372-79750927", "18372-79750923", "18372-79750925")

    val a = baseStationList.contains(null)
    println(a)
    println("00".toInt)
  }
}