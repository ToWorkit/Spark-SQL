package com.logs.log

import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}

/**
  * ==> 转换
  * 访问日志转换(输入 ==> 输出)工具类
  */
object AccessConvertUtil {
  // 数据结构信息
  val struct = StructType(
    Array(
      StructField("url", StringType),
      StructField("cmsType", StringType),
      StructField("cmsId", LongType),
      StructField("traffic", LongType),
      StructField("ip", StringType),
      StructField("city", StringType),
      StructField("time", StringType),
      StructField("day", StringType)
    )
  )

  /**
    * 根据输入的每一行信息转换成输出的样式
    * @param log 输入的每一行记录信息
    */
  def parseLog(log: String) = {
    val splits = log.split("\t")

    val url = splits(1)
    val traffic = splits(2).toLong
    val ip = splits(3)

    // 固定的信息
    val domain = "http://"
  }
}
