package com.logs.log
/**
  * IP解析工具类
  */
object IpUtils {
  def getCity(ip: String) = {
    ip(ip.length - 1) match {
      case '1' => "北京市"
      case '2' => "浙江省"
      case '3' => "上海市"
      case '4' => "陕西省"
      case '5' => "广东省"
      case '6' => "杭州市"
      case '7' => "成都市"
      case '8' => "重庆省"
      case '9' => "沈阳市"
      case '0' => "武汉市"
    }
  }

  def main(args: Array[String]): Unit = {
    println(getCity("222.129.235.186"))
  }
}
