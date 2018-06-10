package com.logs.spark

import org.apache.spark.sql.SparkSession

object DatasetApp {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("DatasetApp").master("local[2]").getOrCreate()

    val path = "C:\\Users\\Just Do It\\Desktop\\iommc_data\\data\\sales.csv"
    // spark解析csv文件
    val df = spark.read.option("header", "true").option("inferSchema", "true").csv(path)
    df.show()

  }
}
