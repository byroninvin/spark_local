package demo.sparksql

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
 * Created by Joe.Kwan on 2020/3/18
 */
object SparkSqlGroupByArray {

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

    val data = Seq(
      ("id1", "vid01", 11.5),
      ("id1", "vid02", 23.4),
      ("id1", "vid03", 10.8),
      ("id2", "vid02", 0.4),
      ("id2", "vid03", 0.5),
      ("id2", "vid04", 2.5)).toDF("did", "vid", "score")

//    data.show()

    /**
     * +---+-----+-----+
     * |did|  vid|score|
     * +---+-----+-----+
     * |id1|vid01|  1.5|
     * |id1|vid02|  2.4|
     * |id1|vid03|  0.8|
     * |id2|vid02|  0.4|
     * |id2|vid03|  0.5|
     * |id2|vid04|  2.5|
     * +---+-----+-----+
     */

    val mergeList = udf{(str: Seq[String]) => str.map(_.split(":")(1)).toList.mkString(",")}

    val df01 = data.withColumn("connect", concat_ws(":", col("score"), col("vid")))
        .groupBy("did").agg(sort_array(collect_list("connect"), asc = false).alias("vids"))




    /**
     *
     */

    val data2 = Seq(
      ("id1", "vid05", 11.5),
      ("id1", "vid06", 0.4),
      ("id1", "vid07", 10.8),
      ("id2", "vid03", 0.4),
      ("id2", "vid04", 0.5),
      ("id2", "vid01", 2.5)).toDF("did", "vid", "score")
    val df02 = data2.withColumn("connect", concat_ws(":", col("score"), col("vid")))
      .groupBy("did").agg(sort_array(collect_list("connect"), asc = false).alias("vids"))

    //
    val flattenDistinct = udf((xs: Seq[Seq[String]])=> xs.flatten.distinct)
    val mergeListDistinct = udf{(str: Seq[String]) => str.map(_.split(":")(1)).distinct.toList.mkString(",")}


    df01.union(df02).show(false)

    df01.union(df02).groupBy("did").agg(sort_array(flattenDistinct(collect_list("vids")), asc = false).alias("vids"))
      .withColumn("vids", mergeListDistinct(col("vids")))

      // 用这个方法来处理后续的任务
      .withColumn("vid_test", split(col("vids"), ","))
      .show(false)


    spark.stop()
  }

}
