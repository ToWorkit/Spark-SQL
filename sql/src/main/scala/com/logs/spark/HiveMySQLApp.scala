package com.logs.spark

import org.apache.spark.sql.SparkSession

/**
  * 使用外部数据源综合查询Hive和MySQL的表数据
  */
object HiveMySQLApp {
  def main(args: Array[String]): Unit = {
    var spark = SparkSession.builder().appName("HiveMySQLApp").master("local[2]").getOrCreate()

    // 加载Hive表的数据
    // 加载MySQL表
    // join



    spark.stop()
  }
}
