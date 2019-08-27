package coocaa.test.situation4awareness.utils

import java.sql.{Connection, DriverManager, PreparedStatement}

import coocaa.test.situation4awareness.utils.ColumnsTags._

/**
  * Created by Joe.Kwan on 2018/11/8 17:38. 
  */
object MySqlUtil {

  val mysql_url = "jdbc:mysql://192.168.1.57:3307/"
  val mysql_database = "test"
  val mysql_username = "hadoop2"
  val mysql_password = "pw.mDFL1ap"

  def insertMySqlClickData(mysql: String, newData: Array[StatsClickOut]) = {
    var conn: Connection = null
    var ps: PreparedStatement = null
    Class.forName("com.mysql.jdbc.Driver")
    conn = DriverManager.getConnection(s"${mysql_url}${mysql_database}?useUnicode=true&characterEncoding=UTF-8", s"${mysql_username}", s"${mysql_password}")
    conn.setAutoCommit(false)
    ps = conn.prepareStatement(mysql)

    for (elem <- newData) {
      ps.setString(1, elem.video_id)
      ps.setString(2, elem.name)
      ps.setString(3, elem.category)
      ps.setString(4, elem.partition_day)
      ps.setLong(5, elem.sum_dur_hours)
      ps.setLong(6, elem.count_numbers)
      ps.addBatch()
    }
    println("====> metadata: " + ps.getMetaData)
    println("====> resultset: " + ps.getResultSet)
    ps.executeBatch()
    conn.commit()
    ps.close()
    conn.close()
  }


  def insertMySqlTagData(mysql: String, newData: Array[StatsTagOut]) = {
    var conn: Connection = null
    var ps: PreparedStatement = null
    Class.forName("com.mysql.jdbc.Driver")
    conn = DriverManager.getConnection(s"${mysql_url}${mysql_database}?useUnicode=true&characterEncoding=UTF-8", s"${mysql_username}", s"${mysql_password}")
    conn.setAutoCommit(false)
    ps = conn.prepareStatement(mysql)

    for (elem <- newData) {
      ps.setString(1, elem.single_tag)
      ps.setString(2, elem.category)
      ps.setString(3, elem.partition_day)
      ps.setLong(4, elem.sum_dur_hours)
      ps.setLong(5, elem.count_numbers)
      ps.addBatch()
    }
    println("====> metadata: " + ps.getMetaData)
    println("====> resultset: " + ps.getResultSet)
    ps.executeBatch()
    conn.commit()
    ps.close()
    conn.close()
  }



}
