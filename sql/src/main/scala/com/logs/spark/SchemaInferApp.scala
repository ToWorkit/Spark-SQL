package com.logs.spark

import org.apache.spark.sql.SparkSession

/**
  * Schema Infer
  */
object SchemaInferApp {
  def main(args: Array[String]): Unit = {
    var spark = SparkSession.builder().appName("SchemaInferApp").master("local[2]").getOrCreate()
    // 加载数据, 返回值为DataFrame格式数据
    val df = spark.read.format("json").load("C:\\Users\\Just Do It\\Desktop\\iommc_data\\data\\json_schema.json")
    df.printSchema()
    df.show()

    spark.stop()
  }
}
