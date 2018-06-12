package com.logs.log

import java.sql.{Connection, PreparedStatement}

import scala.collection.mutable.ListBuffer

object StatDAO {

  /**
    * 批量保存DayVideoTrafficsStat到数据库
    */
  def insertDayVideoTrafficsAccessTopN(list: ListBuffer[DayVideoTrafficsStat])= {
    var connection: Connection = null
    var pstmt: PreparedStatement = null
    try {
      connection = MySQLUtils.getConnection()

      // 批量操作时设置为手动提交，默认为自动
      connection.setAutoCommit(false)
      // ? 占位符
      val sql = "insert into day_video_traffics_topn_stat(day, cms_id, traffics) values (?, ?, ?)"
      pstmt = connection.prepareStatement(sql)
      for (ele <- list) {
        pstmt.setString(1, ele.day)
        pstmt.setLong(2, ele.cmsId)
        pstmt.setLong(3, ele.traffics)
        // 添加到批量中，按批次提交
        pstmt.addBatch()
      }

      // 执行批量处理
      pstmt.executeBatch()
      // 手动提交
      connection.commit()

    }catch {
      case e: Exception => e.printStackTrace()
    } finally {
      // 关闭掉
      MySQLUtils.release(connection, pstmt)
    }
  }


  /**
    * 批量保存DDayCityVideoAccessStat到数据库
    */
  def insertDayCityVideoAccessTopN(list: ListBuffer[DayCityVideoAccessStat])= {
    var connection: Connection = null
    var pstmt: PreparedStatement = null
    try {
      connection = MySQLUtils.getConnection()

      // 批量操作时设置为手动提交，默认为自动
      connection.setAutoCommit(false)
      // ? 占位符
      val sql = "insert into day_video_city_access_topn_stat(day, cms_id, city, times, times_rank) values (?, ?, ?, ?, ?)"
      pstmt = connection.prepareStatement(sql)
      for (ele <- list) {
        pstmt.setString(1, ele.day)
        pstmt.setLong(2, ele.cmsId)
        pstmt.setString(3, ele.city)
        pstmt.setLong(4, ele.times)
        pstmt.setInt(5, ele.timeRank)
        // 添加到批量中，按批次提交
        pstmt.addBatch()
      }

      // 执行批量处理
      pstmt.executeBatch()
      // 手动提交
      connection.commit()

    }catch {
      case e: Exception => e.printStackTrace()
    } finally {
      // 关闭掉
      MySQLUtils.release(connection, pstmt)
    }
  }


  /**
    * 批量保存DayVideoAccessStat到数据库
    */
  def insertDayVideoAccessTopN(list: ListBuffer[DayVideoAccessStat])= {
    var connection: Connection = null
    var pstmt: PreparedStatement = null
    try {
      connection = MySQLUtils.getConnection()

      // 批量操作时设置为手动提交，默认为自动
      connection.setAutoCommit(false)
      val sql = "insert into day_video_access_topn_stat(day, cms_id, times) values (?, ?, ?)"
      pstmt = connection.prepareStatement(sql)
      for (ele <- list) {
        pstmt.setString(1, ele.day)
        pstmt.setLong(2, ele.cmsId)
        pstmt.setLong(3, ele.times)
        // 添加到批量中，按批次提交
        pstmt.addBatch()
      }

      // 执行批量处理
      pstmt.executeBatch()
      // 手动提交
      connection.commit()

    }catch {
      case e: Exception => e.printStackTrace()
    } finally {
      // 关闭掉
      MySQLUtils.release(connection, pstmt)
    }
  }

  /**
    * 删除指定日志的数据
    * @param day
    */
  def deleteData(day: String) = {
    val tables = Array("day_video_access_topn_stat", "day_video_city_access_topn_stat", "day_video_traffics_topn_stat")

    var connection: Connection = null
    var pstmt: PreparedStatement = null

    try {
      // 建立连接
      connection = MySQLUtils.getConnection()
      for (table <- tables) {
        // 字符串格式化 s， ? 代表 pstmt 传入的参数，下标从1开始
        val deleteSQL = s"delete from $table where day = ?"
        pstmt = connection.prepareStatement(deleteSQL)
        pstmt.setString(1, day)
        pstmt.executeUpdate()
      }
    }catch {
      case e: Exception => e.printStackTrace()
    } finally {
      MySQLUtils.release(connection, pstmt)
    }
  }
}
