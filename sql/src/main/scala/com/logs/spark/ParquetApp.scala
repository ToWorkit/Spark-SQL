package com.logs.spark

import org.apache.spark.sql.SparkSession

/**
  * Parquet文件操作
  */
object ParquetApp {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("ParquetApp").master("local[2]").getOrCreate()
    // 加载 parquet数据
    val userDF = spark.read.format("parquet").load("C:\\Users\\Just Do It\\Desktop\\iommc_data\\data\\users.parquet")
    // 根据源码注释，文件格式以树状形式打印到控制台
    userDF.printSchema()
    // 显示内容
    userDF.show()
    // 查看指定的数据
    userDF.select("name", "favorite_color").show()
    // 提取指定的数据以json格式保存 有待解决bug
    userDF.select("name", "favorite_color").write.format("json").save("C:\\Users\\Just Do It\\Desktop\\iommc_data\\data\\user")

    /**
      * 简写方式
      */
    spark.read.load("C:\\Users\\Just Do It\\Desktop\\iommc_data\\data\\users.parquet").show()

    spark.stop()
  }
}
