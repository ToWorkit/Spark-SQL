package com.logs.spark

import org.apache.spark.sql.SparkSession

/**
  * DataFrame 中的API操作
  */
object DataFrameCase {
  def main(args: Array[String]): Unit = {
    // 1、创建
    val spark = SparkSession.builder().appName("DataFrameCase").master("local[2]").getOrCreate()

    // 2、操作
    // RDD ==> DataFrame
    val rdd = spark.sparkContext.textFile("C:\\Users\\Just Do It\\Desktop\\iommc_data\\data\\student.data")
    // 需要导入隐式转换, 转为DataFrame
    import spark.implicits._
    // 注意转义字符
    val studentDF = rdd.map(_.split("\\|")).map(line => Student(line(0).toInt, line(1), line(2), line(3))).toDF()
    // 30行数据，不截取长的数据
    studentDF.show(30, false)

    // 返回前十行
    studentDF.take(10)
    // 第一条
    studentDF.first()
    // 前三条
    studentDF.head(3)
    // 获取指定列
    studentDF.select("name", "email").show(30, false)
    // 过滤
    studentDF.filter("name=''").show()
    studentDF.filter("name='' OR name='NULL'").show()
    // name以'M'开头的数据 substr 或者 substring 截取
    studentDF.filter("SUBSTR(name, 0, 1)='M'").show()
    // 排序
    studentDF.sort(studentDF("name")).show()
    // 降序
    studentDF.sort(studentDF("name").desc).show()
    // 按照多列排序 name和id
    studentDF.sort("name", "id").show()
    // 按照 name的升序，id的降序
    studentDF.sort(studentDF("name").asc, studentDF("id").desc).show()
    // 重命名
    studentDF.select(studentDF("name").as("sutdent_name")).show()
    // join操作
    val studentDF_2 = rdd.map(_.split("\\|")).map(line => Student(line(0).toInt, line(1), line(2), line(3))).toDF()
    // 注意是 ===
    studentDF.join(studentDF_2, studentDF.col("id") === studentDF_2.col("id")).show()

    // 3、关闭
    spark.stop()
  }
  // 利用反射将RDD数据转为DataFrame
  case class Student(id: Int, name: String, phone: String, email: String)

}
