import java.util

import org.apache.spark.SparkConf
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions._


import scala.util.Random

/**
  * Created by Joe.Kwan on 2019-9-26 15:47. 
  */
object NegativeSampleDemo {

  def takeSample(a:Array[String], n:Int, seed: Long) = {
    val rnd = new Random(seed)
    Array.fill(n)(a(rnd.nextInt(a.size)))
  }


  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder()
      .config("spark.default.parallelism", "600")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.rdd.compress", "true")
      .config("spark.sql.crossJoin.enabled", "true")
      .config("spark.speculation.interval", "10000ms")
      .config("spark.sql.tungsten.enabled", "true")
      .config("spark.sql.shuffle.partitions", "600")
      .config("hive.metastore.uris", "thrift://xl.namenode2.coocaa.com:9083")
      .config("spark.sql.warehouse.dir", "hdfs://coocaadata/apps/hive/warehouse")
      .appName(s"${this.getClass.getSimpleName}")
      .enableHiveSupport()
      .getOrCreate()

    spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")
    //设置orc解析模式 如果分区下没有文件也能在sql也能查询不会抛错
    spark.sqlContext.setConf("spark.sql.hive.convertMetastoreOrc", "false")


    /**
      *
      */

    val userRatingBasic = spark.sql(
      s"""
         |select mac, did, uid, dataValue.vid as vid, dataValue.name as name, dataValue.category as category, cast(dataValue.dur_time as bigint) as dur_time, dataValue.source as source,
         |  dataValue.click_nums as click_nums, dataValue.click_daily_nums as click_daily_nums, dataValue.frist_play_date as frist_play_date, dataValue.last_play_date
         |   from recommend_stage_two.album_user_player_history_for_video lateral view explode(plays) myTable as dataKey, dataValue
         |   where day='2019-09-20' and dataValue.dur_time > 60
      """.stripMargin).repartition(480, col("did"))
    userRatingBasic.createOrReplaceTempView("user_rating_basic")

    val usrPostive = userRatingBasic.select("did", "vid").distinct()

    import spark.implicits._
//    val vidList: Array[Any] = userRatingBasic.select("vid").distinct().collect().map(_(0))
//    val vidList: Array[Any] = userRatingBasic.select("vid").distinct().rdd.map(_(0)).collect()
    val vidArray: Array[String] = userRatingBasic.select("vid").distinct().map(_.getString(0)).collect()



    usrPostive.rdd.map{row => (row.getAs[String]("did"), List(row))}.reduceByKey{ (a, b) => a.:::(b)}.map {
      row =>
        val did = row._1
        val iter = row._2
        val negative_sample_size = iter.size * 2
        val negative_sample_array = takeSample(vidArray, negative_sample_size, 2019L)
        val vid_array = scala.collection.mutable.ArrayBuffer[String]()
        for (it <- iter) {
          vid_array += it.getAs("vid")
        }

        (did, negative_sample_array.diff(vid_array))
    }.toDF("did", "vids").withColumn("vid", explode($"vids")).select("did", "vid").show()







    spark.stop()



  }
}
