package com.logs.log

import org.apache.spark.sql.Row
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

    try {
      val splits = log.split("\t")
      val url = splits(1)
      val traffic = splits(2).toLong
      val ip = splits(3)

      // 固定的信息 主站域名
      // http://www.imooc.com/video/14623
      val domain = "http://www.imooc.com/"

      // video/14623
      val cms = url.substring(url.indexOf(domain) + domain.length)
      val cmsTypeId = cms.split("/")

      /*    val cmsType = ""
          // 零 L Long类型
          val cmsId = 0l*/

      var cmsType = ""
      var cmsId = 0l
      if (cmsTypeId.length > 1) {
        // 如果上面是 val 则会报错 Reassignment to val，不能为常量赋值，需要改为 var
        cmsType = cmsTypeId(0)
        cmsId = cmsTypeId(1).toLong
      }
      val city = IpUtils.getCity(ip)
      val time = splits(0)
      val day = time.substring(0, 10).replaceAll("-", "")

      // row类型格式数据，里面的字段必须和struct中的字段对应上
      Row(url, cmsType, cmsId, traffic, ip, city, time, day)
    } catch {
      case e: Exception => Row(0)
    }
  }
}
