package coocaa.test.recommend4movie

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * Created by Joe.Kwan on 2018/12/14 10:26.
  */
object movieAlsItem2ItemDemo {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    /**
      * 导入配置文件
      */
    val sparkConf = new SparkConf()

    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    sparkConf.set("spark.rdd.compress", "true")
    sparkConf.set("hive.metastore.warehouse.dir", "hdfs://coocaadata/apps/hive/warehouse")
    sparkConf.set("spark.sql.warehouse.dir", "hdfs://coocaadata/apps/hive/warehouse")
    val spark = SparkSession.builder()
      .config(sparkConf)
      .appName(s"${this.getClass.getSimpleName} guanyue".filter(!_.equals('$')))
      .enableHiveSupport()
      .master("local[2]")
      .getOrCreate()
    spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")

    /**
      * 日期函数
      */
    val outputartitionDate = "2019-04-02"
    val date_Str = "2019-04-02"


    val oriDF = spark.sql(
      s"""
         |SELECT mac as did, video_id, dur, (1.0 + (dur - min_dur) / (max_dur - min_dur) * 4.0) AS rating, count_n
         |FROM
         |
         |(
         |  SELECT mac, video_id, dur,
         |         max(dur) over(partition by mac) as max_dur,
         |         min(dur) over(partition by mac) as min_dur,
         |         count(dur) over(partition by mac) as count_n
         |  FROM recommendation.rating_data_user_movie_test
         |  WHERE dt = '${outputartitionDate}') a
         |
         |WHERE count_n >= ( 20 + cast(rand() * 10 as int) )
         |limit 1000
        """.stripMargin)
      .select("did", "video_id", "rating")
      .selectExpr("cast(did as String) did",
        "cast(video_id as String) video_id",
        "cast(rating as Double) rating")


    oriDF.show()


    /**
      * 写入mysql中
      */

//    val url = "jdbc:mysql://192.168.1.57:3307/test?useUnicode=true&characterEncoding=UTF-8"
//
//    val pro = new Properties()
//    pro.put("user","hadoop2")
//    pro.put("password","pw.mDFL1ap")
//    pro.put("driver", "com.mysql.jdbc.Driver")
//    usrPlayerDayVideoInfo.write.mode("Overwrite").jdbc(url,"usr_player_day_video_info",pro)


    spark.stop()
  }
}
