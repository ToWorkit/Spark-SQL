package com.logs.spark

import org.apache.spark.sql.SparkSession

/**
  * DataFrame API基本操作
  */
object DataFrameApp {
  def main(args: Array[String]): Unit = {
    // 1、创建
    val spark = SparkSession.builder().appName("DataFrameApp").master("local[2]").getOrCreate()

    // 2、操作 将json文件加载成一个DataFrame
    val peopleDF = spark.read.format("json").load("C:\\Users\\Just Do It\\Desktop\\iommc_data\\people.json")
    // 输出DataFrame对应的信息
    peopleDF.printSchema()
    // 输出内容, 默认展示20条，可以传入参数
    peopleDF.show()
    // 查询某列所有的数据：select name from table;
    peopleDF.select("name").show()
    // 查询多列所有的数据，并且对列进行计算，select name, age + 10 as age_2 from table;
    peopleDF.select(peopleDF.col("name"), (peopleDF.col("age") + 10).as("age_2")).show()
    // 根据某一列的值进行过滤：select * from table where age > 19
    peopleDF.filter(peopleDF.col("age") > 19).show()
    // 根据某一列进行分组，然后再进行聚合操作：select age, count(1) from table group by age;
    peopleDF.groupBy("age").count().show()

    // 3、关闭
    spark.stop()
  }
}
