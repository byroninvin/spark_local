package ml.recommend2demo

import java.util.Properties

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}


object RecommenRatingTest {


  def main(args: Array[String]): Unit = {

    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
    val spark: SparkSession = SparkSession.builder()
      .master("local[*]").appName(s"${this.getClass.getSimpleName}")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()


    val df1 = spark.read.option("header", true).csv("F:\\projectDB\\Coocaa\\RecommendationSystem\\Rating\\RatingTest.csv").toDF()

    // 如果数据需要去重的话,需要如下预处理
    val df2 = df1.dropDuplicates(Seq("mac", "video_id", "start_time", "stop_time"))

    df2.createOrReplaceTempView("edw_play_test")

    // 自定义一个归一化函数
    spark.udf.register("rating_mmn", (aRating: Double, minSocre: Double, maxScore: Double, level: Int) => {
      var resultRating = level + (aRating - minSocre) / (maxScore - minSocre)
      resultRating
    })




    // group_sum(dur) / 5600  ==> rating1
    val df3: DataFrame = spark.sql("select mac, video_id, sum(dur) as sum_dur, round(sum(dur)/5600,10) as rating1, max(day) as day from (select mac, video_id, (to_unix_timestamp(stop_time) - to_unix_timestamp(start_time)) AS dur, day from edw_play_test) where dur > 300 group by mac, video_id")
    df3.createOrReplaceTempView("edw_play_test2")


    /**
      * 分位数sql算法
      */
//    spark.sql(
//      """
//        |select
//        | max(case percentile_rank when 1 then rating1 else 0 END) AS quantile_25,
//        | max(case percentile_rank when 2 then rating1 else 0 END) AS quantile_50,
//        | max(case percentile_rank when 3 then rating1 else 0 END) AS quantile_75,
//        | max(case percentile_rank when 4 then rating1 else 0 END) AS quantile_100
//        |from (
//        |          select mac, video_id, sum_dur, day, rating1, row_number() over(order by rating1) percentile_rank
//        |              from
//        |              (select mac, video_id, sum_dur, day, rating1, rank, total_num
//        |                  from
//        |                  (select mac, video_id, sum_dur, day, rating1, rank, (select count(rating1) from edw_play_test2) as total_num
//        |                      from
//        |                      (select mac, video_id, sum_dur, day, rating1, row_number() over(order by rating1) as rank from edw_play_test2)
//        |                  )
//        |                  where rank=ceil(total_num*0.25) or rank=ceil(total_num*0.5) or rank=ceil(total_num*0.75) or rank=total_num
//        |              ) percentile_table
//        |       )
//      """.stripMargin).show(50)


    /**
      * 分位数Hql自带函数(据说在集群上运行效果不行
      */

    val quantileResultRow: Row = spark.sql("select percentile_approx(rating1, 0.25) as quantile_25, " +
      "percentile_approx(rating1, 0.5) as quantile_50, " +
      "percentile_approx(rating1, 0.75) as quantile_75," +
      "percentile_approx(rating1, 1.0) as quantile_1 from edw_play_test2").collectAsList().get(0)
    val quantile_25: Any = quantileResultRow.get(0)     // double
    val quantile_50: Any = quantileResultRow.get(1)
    val quantile_75: Any = quantileResultRow.get(2)
    val quantile_100: Any = quantileResultRow.get(3)    // quantile_100  == max(rating1)


    /**
      * 使用分位数对启播的每一条观影记录重新打分
      */
    val df4 = spark.sql("select mac, video_id, day, sum_dur, rating1, " +
      "(case " +
      s"     when (rating1 <= $quantile_25) then rating_mmn(rating1, 0, $quantile_25, 1) " +
      s"     when ((rating1 > $quantile_25) and (rating1 <= $quantile_50)) then rating_mmn(rating1, $quantile_25, $quantile_50, 2) " +
      s"     when ((rating1 > $quantile_50) and (rating1 <= $quantile_75)) then rating_mmn(rating1, $quantile_50, $quantile_75, 3) " +
      s"     when ((rating1 > $quantile_75) and (rating1 <= $quantile_100)) then rating_mmn(rating1, $quantile_75, $quantile_100, 4) " +
      s"end) AS rating2 from edw_play_test2")

    /**
      * 将数据保存到本地,用于测试
      */
//      val df4 = spark.sql("select mac, video_id, day, sum_dur, rating1, " +
//      "(case " +
//      s"     when (rating1 <= $quantile_25) then rating_mmn(rating1, 0, $quantile_25, 1) " +
//      s"     when ((rating1 > $quantile_25) and (rating1 <= $quantile_50)) then rating_mmn(rating1, $quantile_25, $quantile_50, 2) " +
//      s"     when ((rating1 > $quantile_50) and (rating1 <= $quantile_75)) then rating_mmn(rating1, $quantile_50, $quantile_75, 3) " +
//      s"     when ((rating1 > $quantile_75) and (rating1 <= $quantile_100)) then rating_mmn(rating1, $quantile_75, $quantile_100, 4) " +
//      s"end) AS rating2, '2018-01-01' as dt from edw_play_test2")
//      .write.option("header", true).csv("F:\\projectDB\\Coocaa\\RecommendationSystem\\Rating\\RatingTestResult.csv")


    /**
      * 用于查看
      * 生成最终rating的结果文件,加入每个mac的分组降序评分
      */
//    df4.createOrReplaceTempView("edw_play_test3")
//    // 测试最终结果查看
//    spark.sql(
//      """
//        |select *, row_number() over(partition by mac order by rating3 desc) as edw_player_rank
//        |from
//        |(
//        |select mac, video_id, day, sum_dur, rating1, rating2, datediff(max_day,day) diff_day,
//        |case
//        | when (rating2 * power(0.98, datediff(max_day,day))) > 1 then (rating2 * power(0.98, datediff(max_day,day)))
//        | else 1
//        |end as rating3
//        |from
//        |(select *,(select max(day) from edw_play_test3) as max_day from edw_play_test3)
//        |)
//      """.stripMargin)


    /**
      * 将数据Jdbc存入数据库
      *
      */
//    val props = new Properties()
//    props.put("user", "hadoop2")
//    props.put("password", "pw.mDFL1ap")
//    //
//    spark.sql(
//      """
//        |select *, row_number() over(partition by mac order by rating2 desc) as edw_player_rank
//        |from
//        |(
//        |select mac, video_id, day, sum_dur, rating1, rating2, datediff(max_day,day) diff_day,
//        |case
//        | when (rating2 * power(0.98, datediff(max_day,day))) > 1 then (rating2 * power(0.98, datediff(max_day,day)))
//        | else 1
//        |end as rating3
//        |from
//        |(select *,(select max(day) from edw_play_test3) as max_day from edw_play_test3)
//        |)
//      """.stripMargin).write.mode("Overwrite")
//      .jdbc("jdbc:mysql://192.168.1.57:3307/test", "recommendation_rating_test_result",props)
//




    spark.stop()

  }

}
