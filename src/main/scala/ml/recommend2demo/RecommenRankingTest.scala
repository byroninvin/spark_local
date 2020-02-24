package ml.recommend2demo

import java.util.Properties

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}



object RecommenRankingTest {

  def getMysalProperty: Properties = {
    val prop = new Properties()
    prop.setProperty("user", "hadoop2")
    prop.setProperty("password", "pw.mDFL1ap")
    prop.setProperty("driver", "com.mysql.jdbc.Driver")
    prop
  }

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
    val spark: SparkSession = SparkSession.builder()
      .master("local[*]").appName("recommen_ranking_test")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()

    val df1 = spark.read.option("header", true).csv("F:\\projectDB\\Coocaa\\RecommendationSystem\\Rating\\RatingTestResult.csv").toDF()

    df1.createOrReplaceTempView("edw_play_test3")

    val df2: DataFrame = spark.sql("select * from " +
      "(select mac, video_id, day, sum_dur, rating1, rating2, row_number() over(partition by mac order by rating2 desc) as rank from edw_play_test3) " +
      "where rank <=10 and mac ='00000000088D'")           // limit 1000

    df2.createOrReplaceTempView("edw_play_test4")
//    val df3 = df2.selectExpr("mac","video_id", "day", "sum_dur", "rating1", "cast(rating2 as Double) rating2", "rank")
//    df3.createOrReplaceTempView("edw_play_test4")

    // 导入相似度数据
//    val df3: DataFrame = spark.read.jdbc("jdbc:mysql://192.168.1.57:3307/test","re_item_to_item_sim_test",getMysalProperty)
//    df3.write.option("header", true).csv("F:\\projectDB\\Coocaa\\RecommendationSystem\\Rating\\RankingTestSim.csv")

    val df4 = spark.read.option("header", true).csv("F:\\projectDB\\Coocaa\\RecommendationSystem\\Rating\\RankingTestSim.csv").toDF()
    df4.createOrReplaceTempView("ranking_test_sim")

    val df5: DataFrame = spark.sql("select iid, sim[0] as iid_recommend, sim[1] as similarity from (select iid as iid, split(sim,':') as sim from ranking_test_sim)")
    df5.createOrReplaceTempView("ranking_test_sim2")

    // 修改 similarity类型
//    val df6: DataFrame = df5.selectExpr("iid", "iid_recommend", "cast(similarity as Double) similarity")
//    df6.createOrReplaceTempView("ranking_test_sim2")

//    val df7: DataFrame = spark.sql("select iid, iid_recommend, pow(similarity/10, 2.0) * 100 as similarity from ranking_test_sim2")
//    df7.createOrReplaceTempView("ranking_test_sim3")



//    spark.sql("select mac, video_id, b.iid_recommend, nvl(sum(a.rating2 * b.similarity)/sum(b.similarity),0) pre_rating from edw_play_test4 a " +
//      "left join ranking_test_sim2 b on a.video_id = b.iid " +
//      "group by a.mac, a.video_id, b.iid_recommend").show(350)



    val df10 = spark.sql(
      """
        |select a.mac, concat_ws('|', collect_set(a.video_id)) as based_id,
        |b.iid_recommend, sum(rating_times_sim) test_a, sum(similarity) test_c, sum(rating_times_sim)/ sum(b.similarity) as test_b
        |from
        |(select a.mac, a.video_id, a.rating2, a.rank, b.iid, b.iid_recommend, b.similarity, a.rating2 * b.similarity as rating_times_sim
        |from edw_play_test4 a
        |left join ranking_test_sim2 b
        |on  a.video_id = b.iid)
        |group by a.mac, b.iid_recommend
        |order by test_b desc, test_c desc
      """.stripMargin)

    df10.createOrReplaceTempView("result_test1")

    spark.sql("select mac, based_id, iid_recommend, test_a, test_c, test_b, row_number() over(partition by mac order by test_b desc, test_a desc) as rank " +
      "from result_test1").show(5000)


    spark.stop()

  }

}
