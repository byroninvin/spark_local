package coocaa.test.recommend4movie

import java.util.Properties

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * Created by Joe.Kwan on 2018/12/14 10:26.
  */
object UserPlayerHistoryNew {

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
    val day = "2018-12-18"

    /**
      * ETL
      */
    //创建一个UDF判断需要取出的影片名, 下面要用到
    spark.udf.register("del_name", (title: String) => {

      var statu = 0
      val list_del = List("3D", "HDR", "Dolby Vision", "DRM", "4K")

      for (i <- list_del.indices) {
        if (title.contains(list_del(i))) {

          statu = 1
        }
      }

      statu
    })


    //从媒资库进行第一层筛选
    val videoInfo = spark.sql(
      s"""select b.coocaa_v_id as vid,
         |b.third_source as source,
         |b.tvod as tvod,
         |b.vip as vip
         |from
         |(select coocaa_v_id,source
         |from ods.ods_cc_video_2018
         |where category_name='电影'
         |and is_positive=0
         |and status=1
         |and del_name(title)=0
         |and source in ('yinhe','tencent','yinhe,tencent','tencent,yinhe')) as a
         |join
         |(select coocaa_v_id, third_id, third_source, is_tvod as tvod, is_vip as vip,
         |case when price>0 then 1 else 0 end as payment
         |from ods.ods_cc_video_source_2018
         |where
         |status=1
         |and is_3d=0
         |and third_source in ('yinhe','tencent')) as b
         |on a.coocaa_v_id = b.coocaa_v_id
         |limit 5000
       """.stripMargin).dropDuplicates(Array("vid", "source", "tvod", "vip")).createOrReplaceTempView("video_info_tbl")


    // 取出用户当日观影记录--这里选取时间
    val usrPlayerDay = spark.sql(
      """
        |select mac, did, uid, video_id as vid, name, source, cast(dur as bigint) as dur_time,partition_day as day
        |from
        |edw.edw_player_test
        |where partition_day='2018-12-31'
        |and dur <> 'null'
        |and video_id <> ''
        |and `name` <> ''
        |and `name` is not null
        |and source is not null
        |group by mac, did, uid, video_id, name, source, start_time, stop_time, dur, partition_day  --需要去重
        |limit 5000
      """.stripMargin).createOrReplaceTempView("usr_player_tbl")


    // 关联
    val usrPlayerDayVideoInfo = spark.sql(
      """
        |select MAX(mac) as mac, did, MAX(uid) as uid, vid, MAX(name) as name, sum(dur_time) as dur_time, MAX(source) as source, vip_state,
        | COUNT(1) as click_nums, 1 as click_daily_nums,
        | min(day) as frist_play_date, max(day) as last_play_date, max(day) as day
        |from
        |(
        |select a.mac as mac, a.did as did, a.uid as uid, a.vid as vid, a.name as name, a.source as tv_source, a.dur_time as dur_time, a.day as day,
        |b.source as source, b.tvod as tvod, b.vip as vip,
        | case
        | when (b.vip=0 and b.tvod=0) then 'NVip'
        | when (b.vip=0 and b.tvod=0) then 'NVip'
        | else 'SV' END  vip_state
        |from usr_player_tbl a
        |join video_info_tbl b
        |on a.vid = b.vid and a.source = b.source ) a
        |group by did, vid, vip_state
      """.stripMargin)
//      .createOrReplaceTempView("usr_player_with_Vip")



//
//    import spark.implicits._
//
//    data.map{row => (row.getAs[String]("did"), List(row)) }.reduceByKey{(a, b) => a.:::(b)}.map {
//      row =>
//        val did = row._1
//        val iter = row._2
//        val maps = scala.collection.mutable.Map[String, play_record]()
//        for (it <- iter) {
//          maps.put(it.getAs[String])
//        }
//
//    }




    /**
      * 写入mysql中
      */

    val url = "jdbc:mysql://192.168.1.57:3307/test?useUnicode=true&characterEncoding=UTF-8"

    val pro = new Properties()
    pro.put("user","hadoop2")
    pro.put("password","pw.mDFL1ap")
    pro.put("driver", "com.mysql.jdbc.Driver")
    usrPlayerDayVideoInfo.write.mode("Overwrite").jdbc(url,"usr_player_day_video_info",pro)


    spark.stop()
  }
}
