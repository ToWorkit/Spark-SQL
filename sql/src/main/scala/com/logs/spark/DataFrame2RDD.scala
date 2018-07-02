package com.logs.spark

import org.apache.spark.sql.SparkSession

/**
  * DataFrame 和 RDD 的相互操作
  * RDD ==> DataFrame
  */
object DataFrame2RDD {
  def main(args: Array[String]): Unit = {
    // 1、创建
    val spark = SparkSession.builder().appName("DataFrame2RDD").master("local[2]").getOrCreate()
    // 2、操作
    // RDD
    val rdd = spark.sparkContext.textFile("C:\\Users\\Just Do It\\Desktop\\data\\imooc_spark_sql\\infos.txt")

    // 需要导入隐式转换
    import spark.implicits._
    // 转为DataFrame
    val infoDF = rdd.map(_.split(",")).map(line => Info(line(0).toInt, line(1), line(2).toInt)).toDF()
    infoDF.show()
/*  +---+----+---+
    | id|name|age|
    +---+----+---+
    |  1| jop| 21|
    |  2| kin| 23|
    |  3| loi| 20|
    +---+----+---+*/

    // 过滤年龄小于20岁的
    infoDF.filter(infoDF.col("age") < 20).show()

    // 转换为表进行操作，必须以视图(view)的方式
    infoDF.createOrReplaceTempView("infos")
    // 然后就可以使用spqrkSQL来操作了
    spark.sql("select * from infos where age > 20").show()


    // 3、关闭
    spark.stop()
  }
  // 使用反射 case 样本类模式匹配
  case class Info(id: Int, name: String, age: Int)
}
