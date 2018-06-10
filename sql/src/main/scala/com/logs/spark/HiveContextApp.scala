package com.logs.spark
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext

/**
  * Spark 版本 1.6.1
  * HiveContext的使用
  * 脚本中使用 --jars 将mysql的jar包导入(放在hive/lib目录下的那个)
  */
object HiveContextApp {
  def main(args: Array[String]): Unit = {
    // 1、创建相应的context
    val sparkConf = new SparkConf()
    // 在测试或者生产中，AppName和Master是通过脚本进行指定
    // sparkConf.setAppName("SQLContextAPP").setMaster("local[2]")

    // 通过源码查看SparkContext需要SparkConf作为参数
    val sc = new SparkContext(sparkConf)
    // 通过源码查看HiveContext需要SparkContext作为参数
    val hiveContext = new HiveContext(sc)
    // 2、相关操作
    hiveContext.table("emp").show()
    // 3、关闭资源
    sc.stop()
  }
}
