package coocaa.test.recommend4movie

import java.util.Properties

import coocaa.test.common.DateUtil
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * Created by Joe.Kwan on 2018/12/17 18:07.
  */
object UserPlayerHistoryNewPartTwo {


  // 需要设置这个getMysalProperty
  def getMysalProperty: Properties = {
    val prop = new Properties()
    prop.setProperty("user", "hadoop2")
    prop.setProperty("password", "pw.mDFL1ap")
    prop.setProperty("driver", "com.mysql.jdbc.Driver")
    prop
  }


  case class play_record(vid: String, name: String, vip_state: String, dur_time: String, source: String, click_nums: Long, click_daily_nums: Long, frist_play_date: String, last_play_date: String)
  case class user_play_history(did: String, mac: String, uid: String, plays: Map[String, play_record], day:String)


  def main(args: Array[String]): Unit = {


    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    /**
      * 导入配置文件
      */
    val sparkConf = new SparkConf()

    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    sparkConf.set("spark.rdd.compress", "true")
//    sparkConf.set("hive.metastore.warehouse.dir", "hdfs://coocaadata/apps/hive/warehouse")
//    sparkConf.set("spark.sql.warehouse.dir", "hdfs://coocaadata/apps/hive/warehouse")
    val spark = SparkSession.builder()
      .config(sparkConf)
      .appName(s"${this.getClass.getSimpleName}")
      .enableHiveSupport()
      .master("local[*]")
      .getOrCreate()


    /**
      * loading data from jdbc
      * tableName: usr_player_day_video_info
      */


    val inputDateBeforeOneDay = sparkConf.get("spark.input.date", DateUtil.getDateBeforeDay)



//    val usrPlayerDayVideoInfo = spark.read.jdbc("jdbc:mysql://localhost:3306/test","usr_player_day_video_info",getMysalProperty)
    val usrPlayerDayVideoInfo = spark.read.jdbc("jdbc:mysql://192.168.1.57:3307/test","usr_player_day_video_info",getMysalProperty)
    usrPlayerDayVideoInfo.createOrReplaceTempView("usr_player_day_video_info")
//    usrPlayerDayVideoInfo.show()



    val data = spark.sql(
      s"""
         |select MAX(mac) as mac, did, MAX(uid) as uid, vid, name, vip_state, cast(sum(dur_time) as string) as dur_time, MAX(source) as source,
         |   SUM(click_nums) as click_nums, SUM(click_daily_nums) as click_daily_nums, MIN(frist_play_date) as frist_play_date, MAX(last_play_date) as last_play_date
         |
         |from (
         |
         |select mac, did, uid, dataValue.vid as vid, dataValue.name as name, dataValue.vip_state as vip_state, cast(dataValue.dur_time as bigint) as dur_time, dataValue.source as source,
         |  dataValue.click_nums as click_nums, dataValue.click_daily_nums as click_daily_nums, dataValue.frist_play_date as frist_play_date, dataValue.last_play_date
         |   from recommendation.user_play_history_movie_new lateral view explode(plays) myTable as dataKey, dataValue where day='2018-12-28'  --选取抽取的日期, 一般是计算日的前2天的日期, 输入日期的前一天日期
         |
         |union
         |
         |--从ETL(启播记录中计算的数据)
         |select mac, did, uid, vid, name, vip_state, cast(dur_time as bigint) as time, source,
         |  click_nums, click_daily_nums, frist_play_date, last_play_date
         |
         |   from usr_player_day_video_info  ) as a
         |
         |where did is not null
         |group by did, vid, name, vip_state
      """.stripMargin)


    import spark.implicits._
    // getAs[String]
    val data1 = data.rdd.map {row => (row.getAs[String]("did"), List(row))}.reduceByKey{ (a, b) => a.:::(b)}.map{
      row =>
        val did = row._1
        val iter = row._2
        val maps = scala.collection.mutable.Map[String, play_record]()
        for (it <- iter) {
          maps.put(it.getAs("vid"),
            play_record(
              it.getAs("vid"),
              it.getAs("name"),
              it.getAs("vip_state"),
              it.getAs("dur_time"),
              it.getAs("source"),
              it.getAs("click_nums"),
              it.getAs("click_daily_nums"),
              it.getAs("frist_play_date"),
              it.getAs("last_play_date")
            )
          )
        }
        user_play_history(did, "null", "null", maps.toMap, inputDateBeforeOneDay)            // 输入日或者, 计算日的前一天
    }.toDF().filter("did !=''")


    data1.show()

//
//    spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")
//
//    data1.repartition(80)
//      .write.mode(SaveMode.Overwrite)
//      .option("codec", "org.apache.hadoop.io.compress.SnappyCodec")
//      .partitionBy("day")
//      .insertInto("edw.user_play_history_bg")

    spark.stop()


  }

}
