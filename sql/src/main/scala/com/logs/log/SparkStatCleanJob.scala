package com.logs.log

import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * 使用Spark完成数据清洗操作
  */
object SparkStatCleanJob {
  def main(args: Array[String]): Unit = {
    //设置Hadoop的环境变量
    System.setProperty("hadoop.home.dir", "D:\\hadoop-2.7.3\\hadoop-2.7.3")

    // 1、创建
    val spark = SparkSession.builder().appName("SparkStatCleanJob").master("local[2]").getOrCreate()
    // 2、操作
    val accessRDD = spark.sparkContext.textFile("C:\\Users\\Just Do It\\Desktop\\iommc_data\\data\\access.log")

    // 查看前十条
    // accessRDD.take(10).foreach(println)

    // 将RDD 转为 DataFrame
     val accessDF = spark.createDataFrame(accessRDD.map(x => AccessConvertUtil.parseLog(x)), AccessConvertUtil.struct)
/*      accessDF.printSchema()
      accessDF.show()*/
    // 按 day 分区
    // coalesce 分区但不分文件(都在一个文件内)
    // mode 每次重写
    accessDF.coalesce(1).write.format("parquet").partitionBy("day").mode(SaveMode.Overwrite).save("C:\\Users\\Just Do It\\Desktop\\iommc_data\\data\\test\\")

    // 3、关闭
    spark.stop()
  }
}
