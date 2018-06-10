package com.logs.spark

import org.apache.spark.sql.SparkSession

/**
  * 第一步清洗数据
  *   抽取出需要的指定列的数据
  */
object SparkStatFormatJob {
  def main(args: Array[String]): Unit = {
    // 1、创建
    val spark = SparkSession.builder().appName("SparkStatFormatJob").master("local[2]").getOrCreate()
    // 2、操作
    val access = spark.sparkContext.textFile("C:\\Users\\Just Do It\\Desktop\\iommc_data\\data\\access.20161111.log")
    // 查看10行
//     access.take(10).foreach(println)

    access.map(line => {
      // 使用" "分割
      val splits = line.split(" ")
      // ip
      val ip = splits(0)

      /**
        * 原始日志的第三个和第四个字段拼接起来才是完整的访问时间
        * [10/Nov/2016:09:01:20 +0800] 转成 yyyy-MM-dd HH:mm:ss
        */
      val time = splits(3) + " " + splits(4)
      val url = splits(11).replaceAll("\"", "")
      // 流量
      var traffic = splits(9)
      // 打印
      // (ip, DataUtils.parse(time), url, traffic)
      // 处理一下
      DataUtils.parse(time) + "\t" + url + "\t" + traffic + "\t" + ip
    }).saveAsTextFile("file:///Users/Just Do It/Desktop/iommc_data/data/output/")
    // 打印
    // .take(10).foreach(println)

    // 3、关闭
    spark.stop()
  }
}
