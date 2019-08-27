package coocaa.test.recommend4movie

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * Created by Joe.Kwan on 2019/1/22 9:58. 
  */




object movieRankingNewDemo {

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

    import spark.implicits._
    spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")

    /**
      * 日期函数
      */
    val day = "2019-01-21"


    /**
      * 提取最近一个分区的用户观影历史表
      *
      */


    val lastestPlayerHistory = spark.sql(
      s"""
        |select did,dataValue.vid as vid, dataValue.name as name, dataValue.vip_state as vip_state, cast(dataValue.dur_time as bigint) as dur_time, dataValue.source as source,
        |  dataValue.click_nums as click_nums, dataValue.click_daily_nums as click_daily_nums, dataValue.frist_play_date as frist_play_date, dataValue.last_play_date
        |from recommendation.user_play_history_movie_new lateral view explode(plays) myTable as dataKey, dataValue where day='${day}'
        |limit 100000  --读取前一个已完成的分区时间, 一般是计算日的前两天
      """.stripMargin)


    lastestPlayerHistory.createOrReplaceTempView("lastest_player_history")


    spark.sql(
      """
        |
      """.stripMargin)



    lastestPlayerHistory.show()



















    spark.stop()

  }

}
