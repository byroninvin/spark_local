package demo.sparksql

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

/**
 * Created by Joe.Kwan on 2020/4/8
 */
object SparkSqlReadParquetFile {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
    val spark: SparkSession = SparkSession
      .builder()
      .appName(s"${this.getClass.getSimpleName} guanyue".filter(!_.equals('$')))
      .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .enableHiveSupport()
      .getOrCreate()
    spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")
    // 设置orc解析模式 如果分区下没有文件也能在sql也能查询不会抛错
    spark.sqlContext.setConf("spark.sql.hive.convertMetastoreOrc", "false")

//    val df = spark.read.parquet("/user/guanyue/recommendation/nrt_album/candidate_completion")
//    val df2 = spark.read.parquet("/user/guanyue/recommendation/nrt_album/hotvideodrop")

    val df = spark.read.parquet("/user/bigdata/backup/recommendation/user_history_movie/user_history_movie_2019")



    df.select("user_vid_player").show(1000,false)






    spark.stop()
  }



}
