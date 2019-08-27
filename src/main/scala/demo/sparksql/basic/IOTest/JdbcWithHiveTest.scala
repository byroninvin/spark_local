package demo.sparksql.basic.IOTest

import org.apache.spark.sql.{DataFrame, SparkSession}
import java.sql.DriverManager


import scala.collection.mutable

/**
  * Created by Joe.Kwan on 2018/11/7 9:53. 
  */
object JdbcWithHiveTest {

  case class StatsRec(tab_id: Long, tab_cn_title: String)

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .appName("SparkHiveJdbc")
      .master("local[*]")
      .getOrCreate()

    Class.forName("org.apache.hive.jdbc.HiveDriver")

    /**
      * 方法一.
      */
//    val logs: DataFrame = spark.read.format("jdbc").options(
//      Map("url" -> "jdbc:hive2://192.168.0.159:10000/test",
//        "driver" -> "org.apache.hive.jdbc.HiveDriver",
//        "dbtable" -> "aaa",
//        "user" -> "guanyue",
//        "password" -> "skl7^K23wdt")
//    ).load()
//
//    logs.printSchema()
//    logs.show()

    /**
      * 方法二.
      */
//    Class.forName("org.apache.hive.jdbc.HiveDriver")
//    val conn=DriverManager.getConnection("jdbc:hive2://192.168.0.159:10000","guanyue","skl7^K23wdt")
//    val pstmt= conn.prepareStatement("select * from test.lxx_mall_tab")
//    val rs = pstmt.executeQuery()
//    while (rs.next()){
//      println("mac:"+rs.getString("mac")+",vid:"+rs.getString("vid")+
//        ",score:"+rs.getString("score"))}
//    rs.close()
//    pstmt.close()


//    while(res.next()) {
//      var rec = StatsRec(res.getString("first_name"),
//        res.getString("last_name"),
//        Timestamp.valueOf(res.getString("action_dtm")),
//        res.getLong("size"),
//        res.getLong("size_p"),
//        res.getLong("size_d"))
//      fetchedRes += rec
//    }

    // 生成df
    val conn=DriverManager.getConnection("jdbc:hive2://192.168.0.159:10000","guanyue","skl7^K23wdt")
    val rse= conn.createStatement.executeQuery("select * from test.lxx_mall_tab")

    val fetchedRes = mutable.MutableList[StatsRec]()
    while(rse.next()) {
      var rec = StatsRec(rse.getLong("tab_id"), rse.getString("tab_cn_title"))
      fetchedRes += rec
    }
    conn.close()

    import spark.implicits._
    val rddStatsDelta = spark.sparkContext.parallelize(fetchedRes).toDF()

    rddStatsDelta.show()

    spark.stop()


  }

}
