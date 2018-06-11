package com.logs.log

import org.apache.spark.sql.SparkSession

/**
  * 使用Spark完成数据清洗操作
  */
object SparkStatCleanJob {
  def main(args: Array[String]): Unit = {
    // 1、创建
    val spark = SparkSession.builder().appName("SparkStatCleanJob").master("local[2]").getOrCreate()
    // 2、操作
    val accessRDD = spark.sparkContext.textFile("C:\\Users\\Just Do It\\Desktop\\iommc_data\\data\\access.log")

    // 查看前十条
    // accessRDD.take(10).foreach(println)

    // 将RDD 转为 DataFrame
    // spark.createDataFrame()


    // 3、关闭
    spark.stop()
  }
}
