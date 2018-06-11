package com.logs.log

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

/**
  * topN统计Spark作业
  */
object TopNStatJob {
  def main(args: Array[String]): Unit = {

    // 1、创建
    // config 禁止推倒数据类型，具体查看index.txt
    val spark = SparkSession.builder().appName("TopNStatJob").config("spark.sql.sources.partitionColumnTypeInference.enabled", false).master("local[2]").getOrCreate()
    // 2、操作
    val accessDF = spark.read.format("parquet").load("C:\\Users\\Just Do It\\Desktop\\iommc_data\\data\\test")
/*    accessDF.printSchema()
    accessDF.show()*/

    videoAccessTopNStat(spark, accessDF)

    // 3、关闭
    spark.stop()
  }

  /**
    * 最受欢迎的视频课程TopN
    * @param spark
    * @param accessDF
    */
  def videoAccessTopNStat(spark: SparkSession, accessDF: DataFrame): Unit = {
    /**
      * DataFrame 的方式
      */
    // 单天统计 以 day 和 cmsId 分组
    // 需要隐式转换
    // $"day" 就是一个column
/*    import spark.implicits._
    val videoAccessTopNDF = accessDF.filter($"day" === "20170511" && $"cmsType" === "video").groupBy("day", "cmsId").agg(count("cmsId").as("times")).orderBy($"times".desc)
    videoAccessTopNDF.show(false)*/

    /**
      * Spark SQL 的方式
      */
    accessDF.createOrReplaceTempView("access_logs")
    val videoAccessTopNDF = spark.sql("select day,cmsId,count(1) as times from access_logs where day='20170511' and cmsType='video' group by day,cmsId order by times desc")
    videoAccessTopNDF.show(false)
  }
}
