package com.logs.spark

import java.text.SimpleDateFormat
import java.util.{Date, Locale}

import org.apache.commons.lang3.time.FastDateFormat

/**
  * 日期时间解析工具类
  * 注意：SimpleDateFormat是线程不安全的
  * [10/Nov/2016:09:01:20 +0800] 转成 yyyy-MM-dd HH:mm:ss
  */
object DataUtils {
  // 输入文件日期时间格式：10/Nov/2016:09:01:20 +0800
  val YYYYMMDDHHMM_TIME_FORMAT = FastDateFormat.getInstance("dd/MMM/yyyy:HH:mm:ss Z", Locale.ENGLISH)
  // 需要转换的日期格式
  val TARGET_FORMAT = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss", Locale.ENGLISH)

  /**
    * 获取时间：yyyy-MM-dd HH:mm:ss
    */
  def parse(time: String) = {
    // 由源码可得参数需要long类型
    TARGET_FORMAT.format(new Date(getTime(time)))
  }

  /**
    * 获取输入日志时间：long类型
    * time: [10/Nov/2016:09:01:20 +0800]
    */
  def getTime(time: String) = {
    try {
      // 返回long类型
      YYYYMMDDHHMM_TIME_FORMAT.parse(time.substring(time.indexOf("[") + 1, time.lastIndexOf("]"))).getTime
    } catch {
      case e: Exception => {
        0l
      }
    }
  }
  def main(args: Array[String]): Unit = {
    println(parse("[10/Nov/2016:09:01:20 +0800]"))
  }
}
