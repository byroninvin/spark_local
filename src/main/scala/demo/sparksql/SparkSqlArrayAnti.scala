package demo.sparksql

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
 * Created by Joe.Kwan on 2020/3/18
 */
object SparkSqlArrayAnti {

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

    import spark.implicits._

//    val data = Seq(
//      ("id1", "vid01", 9111.5),
//      ("id1", "vid02", 123.4),
//      ("id1", "vid03", 10.888888),
//      ("id2", "vid02", 0.4),
//      ("id2", "vid03", 0.5),
//      ("id2", "vid04", 2.5)).toDF("did", "vid", "score")
//
//    data.show()
//
//    /**
//     * +---+-----+-----+
//     * |did|  vid|score|
//     * +---+-----+-----+
//     * |id1|vid01|  1.5|
//     * |id1|vid02|  2.4|
//     * |id1|vid03|  0.8|
//     * |id2|vid02|  0.4|
//     * |id2|vid03|  0.5|
//     * |id2|vid04|  2.5|
//     * +---+-----+-----+
//     */
//
//
//    val mergeList = udf{(str: Seq[String]) => str.map(_.split(":")(1)).toList.mkString(",")}
//
//    data.withColumn("connect", concat_ws(":", col("score"), col("vid")))
//        .groupBy("did").agg(sort_array(collect_list("connect"), asc = false).alias("vids"))
//        .withColumn("vids", mergeList(col("vids")))
//        .show(false)


    /**
     * +---+-----------------+
     * |did|vids             |
     * +---+-----------------+
     * |id1|vid01,vid02,vid03|
     * |id2|vid04,vid03,vid02|
     * +---+-----------------+
     */
    val data = Seq(
      ("id1", "vid01,vid02,vid03"),
      ("id2", "vid04,vid03,vid02"),
      ("id3", "vid02,vid04,vid06")
    ).toDF("did", "vids")


    val data_right = Seq(
      ("id1", "vid01,vid02"),
      ("id2", "vid04,vid01")
    ).toDF("did", "vids_drop")


    val antiJoin_udf = udf{(str_a: Seq[String], str_b: Seq[String]) => if (str_b!= null) str_a.diff(str_b) else str_a}


    /**
     * +---+---------------------+--------------+
     * |did|vids                 |vids_drop     |
     * +---+---------------------+--------------+
     * |id1|[vid01, vid02, vid03]|[vid01, vid02]|
     * |id2|[vid04, vid03, vid02]|[vid04, vid01]|
     * |id3|[vid02, vid04, vid06]|null          |
     * +---+---------------------+--------------+
     */


    data.join(data_right, Seq("did"), "left_outer")
      .withColumn("vids", split( $"vids", ",")).withColumn("vids_drop", split($"vids_drop", ","))
      .withColumn("vids_left", antiJoin_udf($"vids", $"vids_drop"))

        .show(false)



    spark.stop()
  }

}
