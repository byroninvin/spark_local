package coocaa.test.situation4awareness

import java.sql.DriverManager

import coocaa.test.situation4awareness.utils.ColumnsTags._
import coocaa.test.situation4awareness.utils.MySqlUtil
import org.apache.spark.sql.SparkSession

import scala.collection.mutable

/**
  * Created by Joe.Kwan on 2018/11/7 10:33.
  */
object SituationAwareness {

  case class StatsEdwPlayer()

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .appName("UDAFTest")
      .master("local[*]")
      .getOrCreate()

    /**
      * 数据处理
      */
    val conn=DriverManager.getConnection("jdbc:hive2://192.168.0.159:10000","guanyue","skl7^K23wdt")
    val rse= conn.createStatement.executeQuery(
      """
        |select
        |a.mac, a.did, a.province, a.city, a.source, a.start_time, a.stop_time, a.dur, a.video_id, a.name, a.category, a.video_source, a.partition_day,
        |b.tag
        |from edw.edw_player_test a
        |left join ods.ods_cc_video_2018 b
        |on a.video_id = b.coocaa_v_id
        |where partition_day='2018-11-06'
        |and dur > 300
        |and `name` is not null
        |limit 8000
      """.stripMargin)


    val fetchedRes = mutable.MutableList[StatsEdwPlayerTest2]()
    while(rse.next()) {
      var rec = StatsEdwPlayerTest2(
        rse.getString("mac"),
        rse.getString("did"),
        rse.getString("province"),
        rse.getString("city"),
        rse.getString("source"),
        rse.getString("start_time"),
        rse.getString("stop_time"),
        rse.getLong("dur"),
        rse.getString("video_id"),
        rse.getString("name"),
        rse.getString("category"),
        rse.getString("video_source"),
        rse.getString("partition_day"),
        rse.getString("tag")
      )
      fetchedRes += rec
    }

    conn.close()

    import spark.implicits._
    val rddStatsDeltaDF = spark.sparkContext.parallelize(fetchedRes).toDF()
    val dailyData = rddStatsDeltaDF.dropDuplicates(Seq("mac", "video_id", "start_time", "stop_time"))
    dailyData.createOrReplaceTempView("daily_data")


    /**
      * 数据处理, 电影点击次数(电影点击次数排名)-可单人多次,需要每次启播时长>300s
      */
    val clickOfMovie= spark.sql(
      """
        |select video_id, `name`, category, partition_day,
        |ceil(sum(dur)/3600) as sum_dur_hours, count(mac) as count_numbers
        |from daily_data
        |where category='电影'
        |group by video_id, `name`, category, partition_day
        |order by sum_dur_hours desc, count_numbers desc
        |limit 100
      """.stripMargin)


    val clickOfTele= spark.sql(
      """
        |select video_id, `name`, category, partition_day,
        |ceil(sum(dur)/3600) as sum_dur_hours, count(mac) as count_numbers
        |from daily_data
        |where category='电视剧'
        |group by video_id, `name`, category, partition_day
        |order by sum_dur_hours desc, count_numbers desc
        |limit 100
      """.stripMargin)


    /**
      * 处理tag数据.
      */

    val tagOfMovie = spark.sql(
      """
        |select single_tag, category, partition_day,
        |ceil(sum(dur)/3600) as sum_dur_hours, count(mac) as count_numbers
        |from daily_data
        |lateral view explode(split(tag, ',')) myTable as single_tag
        |where category='电影'
        |and single_tag in ('喜剧', '搞笑','剧情', '爱情', '动画', '冒险', '儿童', '励志', '成长', '战争', '动作', '犯罪', '冒险', '武侠', '恐怖', '惊悚', '科幻', '悬疑', '奇幻', '暴力', '古装')
        |group by single_tag, category, partition_day
        |order by sum_dur_hours desc, count_numbers desc
      """.stripMargin)


    val tagOfTele = spark.sql(
      """
        |select single_tag, category, partition_day,
        |ceil(sum(dur)/3600) as sum_dur_hours, count(mac) as count_numbers
        |from daily_data
        |lateral view explode(split(tag, ',')) myTable as single_tag
        |where category='电视剧'
        |and single_tag in ('爱情', '年代','家庭', '都市', '战争', '古装', '言情', '情感', '抗日', '军旅', '青春', '传奇', '职场', '历史', '军事', '谍战', '偶像')
        |group by single_tag, category, partition_day
        |order by sum_dur_hours desc, count_numbers desc
      """.stripMargin)



    /**
      * 4份数据输出到两个表中
      *
      */
    val OutClickOfMovie = clickOfMovie.rdd.map(line=> {
      val video_id = line.getAs[String]("video_id")
      val name = line.getAs[String]("name")
      val category = line.getAs[String]("category")
      val partition_day = line.getAs[String]("partition_day")
      val sum_dur_hours = line.getAs[Long]("sum_dur_hours")
      val count_numbers = line.getAs[Long]("count_numbers")
      StatsClickOut(video_id, name, category, partition_day, sum_dur_hours, count_numbers)
    }).collect()


    val OutClickOfTele = clickOfTele.rdd.map(line=> {
      val video_id = line.getAs[String]("video_id")
      val name = line.getAs[String]("name")
      val category = line.getAs[String]("category")
      val partition_day = line.getAs[String]("partition_day")
      val sum_dur_hours = line.getAs[Long]("sum_dur_hours")
      val count_numbers = line.getAs[Long]("count_numbers")
      StatsClickOut(video_id, name, category, partition_day, sum_dur_hours, count_numbers)
    }).collect()


    val OutTagOfMovie = tagOfMovie.rdd.map(line=> {
      val single_tag = line.getAs[String]("single_tag")
      val category = line.getAs[String]("category")
      val partition_day = line.getAs[String]("partition_day")
      val sum_dur_hours = line.getAs[Long]("sum_dur_hours")
      val count_numbers = line.getAs[Long]("count_numbers")
      StatsTagOut(single_tag, category, partition_day, sum_dur_hours, count_numbers)
    }).collect()


    val OutTagOfTele = tagOfTele.rdd.map(line=> {
      val single_tag = line.getAs[String]("single_tag")
      val category = line.getAs[String]("category")
      val partition_day = line.getAs[String]("partition_day")
      val sum_dur_hours = line.getAs[Long]("sum_dur_hours")
      val count_numbers = line.getAs[Long]("count_numbers")
      StatsTagOut(single_tag, category, partition_day, sum_dur_hours, count_numbers)
    }).collect()


    /**
      * 入库
      */
    val sqlClickData = "INSERT INTO rank_click_player_daily (video_id, name, category, partition_day, sum_dur_hours, count_numbers) VALUES (?,?,?,?,?,?)"
    MySqlUtil.insertMySqlClickData(sqlClickData, OutClickOfMovie)
    MySqlUtil.insertMySqlClickData(sqlClickData, OutClickOfTele)

    val sqlTagData = "INSERT INTO rank_tag_player_daily (single_tag, category, partition_day, sum_dur_hours, count_numbers) VALUES (?,?,?,?,?)"
    MySqlUtil.insertMySqlTagData(sqlTagData, OutTagOfMovie)
    MySqlUtil.insertMySqlTagData(sqlTagData, OutTagOfTele)



    spark.stop()
  }

}
