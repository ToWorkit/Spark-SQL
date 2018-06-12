package com.logs.log

import java.sql.{Connection, PreparedStatement}

import scala.collection.mutable.ListBuffer

object StatDAO {

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
}
