package com.logs.spark

import org.apache.spark.sql.SparkSession

object SparkSessionApp {
  def main(args: Array[String]): Unit = {
    // 1、创建
    // http://spark.apache.org/docs/latest/sql-programming-guide.html#starting-point-sparksession
    var spark = SparkSession
      .builder()
      .appName("SparkSessionApp")
      .master("local[2]")
      .getOrCreate()
    // 2、操作
    val prople = spark.read.json("C:\\Users\\Just Do It\\Desktop\\iommc_data\\people.json")
    prople.show()
    // 3、关闭
    spark.stop()
  }
}
