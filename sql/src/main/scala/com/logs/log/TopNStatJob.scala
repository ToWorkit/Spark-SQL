package com.logs.log

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

import scala.collection.mutable.ListBuffer

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

    // 重新写入数据之前先删除之前创建的表
    val day = "20170511"
    StatDAO.deleteData(day)

    // 最受欢迎的TopN课程
    videoAccessTopNStat(spark, accessDF, day)

    // 将城市topN统计结果写入到MySQL中
    cityAccessTopNStat(spark, accessDF, day)

    // 按照流量进行统计
    videoTrafficsTopNStat(spark, accessDF, day)

    // 3、关闭
    spark.stop()
  }

  /**
    * 按照流量进行统计
    * @param spark
    * @param accessDF
    */
  def videoTrafficsTopNStat(spark: SparkSession, accessDF: DataFrame, day: String): Unit = {
    /**
      * DataFrame 的方式
      */
    import spark.implicits._
    val trafficsAccessTopNDF = accessDF.filter($"day" === day && $"cmsType" === "video").groupBy("day", "cmsId").agg(sum("traffic").as("traffics")).orderBy($"traffics".desc)
      //.show(false)

    /**
      * 将统计结果写入到MySQL中
      */
    try {
      // 构建
      trafficsAccessTopNDF.foreachPartition(partitionOfRecords => {
        // 集合操作
        val list = new ListBuffer[DayVideoTrafficsStat]
        partitionOfRecords.foreach(info => {
          val day = info.getAs[String]("day")
          val cmsId = info.getAs[Long]("cmsId")
          val traffics = info.getAs[Long]("traffics")

          list.append(DayVideoTrafficsStat(day, cmsId, traffics))
        })

        StatDAO.insertDayVideoTrafficsAccessTopN(list)
      })
    } catch {
      case e: Exception => e.printStackTrace()
    }
  }

  /**
    * 按照地区进行统计TopN课程
    * @param spark
    * @param accessDF
    */
  def cityAccessTopNStat(spark: SparkSession, accessDF: DataFrame, day: String): Unit = {
    /**
      * DataFrame 的方式
      */
    import spark.implicits._
    val cityAccessTopNDF = accessDF.filter($"day" === day && $"cmsType" === "video").groupBy("day", "city", "cmsId").agg(count("cmsId").as("times"))

    // Window函数在Spark SQL的使用
    // 数据过滤一番
    val top3DF = cityAccessTopNDF.select(
      cityAccessTopNDF("day"),
      cityAccessTopNDF("city"),
      cityAccessTopNDF("cmsId"),
      cityAccessTopNDF("times"),
      row_number().over(Window.partitionBy(cityAccessTopNDF("city")).orderBy(cityAccessTopNDF("times").desc)).as("times_rank")
    ).filter("times_rank <= 3")
      //.show(false)

    /**
      * 将统计结果写入到MySQL中
      */
    try {
      // 构建
      top3DF.foreachPartition(partitionOfRecords => {
        // 集合操作
        val list = new ListBuffer[DayCityVideoAccessStat]
        partitionOfRecords.foreach(info => {
          val day = info.getAs[String]("day")
          val city = info.getAs[String]("city")
          val cmsId = info.getAs[Long]("cmsId")
          val times = info.getAs[Long]("times")
          val times_rank = info.getAs[Int]("times_rank")

          list.append(DayCityVideoAccessStat(day, cmsId, city, times, times_rank))
        })

        StatDAO.insertDayCityVideoAccessTopN(list)
      })
    } catch {
      case e: Exception => e.printStackTrace()
    }
  }

  /**
    * 最受欢迎的视频课程TopN
    * @param spark
    * @param accessDF
    */
  def videoAccessTopNStat(spark: SparkSession, accessDF: DataFrame, day: String): Unit = {
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
    val videoAccessTopNDF = spark.sql(s"select day,cmsId,count(1) as times from access_logs where day=$day and cmsType='video' group by day,cmsId order by times desc")
    // videoAccessTopNDF.show(false)

    /**
      * 将统计结果写入到MySQL中
      */
    try {
      // 构建
      videoAccessTopNDF.foreachPartition(partitionOfRecords => {
        // 集合操作
        val list = new ListBuffer[DayVideoAccessStat]
        partitionOfRecords.foreach(info => {
          val day = info.getAs[String]("day")
          val cmsId = info.getAs[Long]("cmsId")
          val times = info.getAs[Long]("times")

          list.append(DayVideoAccessStat(day, cmsId, times))
        })

        StatDAO.insertDayVideoAccessTopN(list)
      })
    } catch {
      case e: Exception => e.printStackTrace()
    }
  }
}
