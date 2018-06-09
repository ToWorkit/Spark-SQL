package com.logs.spark
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkConf

/**
  * SQLContext的使用
  */
object SQLContextApp {
  def main(args: Array[String]): Unit = {

    val path = args(0)

    // 1、创建相应的Context
    val sparkConf = new SparkConf()
    // 在测试或者生产中，AppName和Master是通过脚本进行指定
    // sparkConf.setAppName("SQLContextAPP").setMaster("local[2]")
    // 通过源码查看SparkContext需要SparkConf作为参数
    val sc = new SparkContext(sparkConf)
    // 通过源码查看SQLContext需要SparkContext作为参数
    val sqlContext = new SQLContext(sc)

    // 2、相关处理: josn
//    val people = sqlContext.read.format("json").load("C:\\Users\\Just Do It\\Desktop\\iommc_data\\people.json")
    val people = sqlContext.read.format("json").load(path)
    people.printSchema()
    people.show()

    // 3、关闭资源
    sc.stop()
  }
}
