package ml

import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.functions.{lit, udf}

/**
 * Created by Joe.Kwan on 2020/5/11
 */
object SparkSparseVectorToWideColumn {

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

    val testDF = spark.read.parquet("/apps/hive/warehouse/user_predict.db/nrt_album_user_features_v1/day=2020-05-09/part-r-00000-d5ea953c-1602-48de-84d3-488fb1b564ae.snappy.parquet")

    /**
     * +------------+-------+--------------------+----------+
     * |         mac| source|        user_feature|       day|
     * +------------+-------+--------------------+----------+
     * |283545585541|  yinhe|(3637,[5,12,19,22...|2020-05-09|
     * |FCA3864A80EA|  yinhe|(3637,[5,18,19,83...|2020-05-09|
     * |FCA3867FDDBE|  yinhe|(3637,[25,60,227,...|2020-05-09|
     * |60427F81F716|  yinhe|(3637,[5,35,41,83...|2020-05-09|
     * |FCA386011471|  yinhe|(3637,[42,97,186,...|2020-05-09|
     * |94903401A878|tencent|(3637,[5,19,70,14...|2020-05-09|
     * |28354599FBB2|  yinhe|(3637,[5,22,157,1...|2020-05-09|
     * |283545F19696|  yinhe|(3637,[289,307,31...|2020-05-09|
     * |BCEC23BAC6DB|  yinhe|(3637,[5,19,41,42...|2020-05-09|
     * |107717A42CD2|tencent|(3637,[25,60,101,...|2020-05-09|
     * |FCA3867F2DA3|  yinhe|(3637,[5,12,25,83...|2020-05-09|
     * |107717425173|tencent|(3637,[5,25,60,69...|2020-05-09|
     * |10771747F692|tencent|(3637,[5,22,69,19...|2020-05-09|
     * |A4E6159ABFC0|tencent|(3637,[5,22,44,70...|2020-05-09|
     * |1077170FCAAF|tencent|(3637,[125,133,14...|2020-05-09|
     * |FCA3869C9A2E|  yinhe|(3637,[186,442,49...|2020-05-09|
     * |283545FFA0BF|  yinhe|(3637,[60,227,420...|2020-05-09|
     * |BCEC23B57FF2|  yinhe|(3637,[77,98,102,...|2020-05-09|
     * |60427F628CA9|tencent|(3637,[25,60,125,...|2020-05-09|
     * |FCA386ACB9BB|tencent|(3637,[5,22,74,77...|2020-05-09|
     * +------------+-------+--------------------+----------+
     */

    // 自定义一个取array-column第i个元素的udf
    val elem = udf((x: Seq[Double], y:Int) => x(y))

    // 自定义个udf
    def split_elem(col: Column, len: Int): Seq[Column] = {
      for (i <- 0 until len) yield { elem(col, lit(i)).as(s"$col($i)")}
    }



    implicit class DataFrameSupport(df: DataFrame) {
      def select(cols: Any*): DataFrame = {
        var buffer: Seq[Column] = Seq.empty
        for (col <- cols) {
          if (col.isInstanceOf[Seq[_]]) {
            buffer = buffer ++ col.asInstanceOf[Seq[Column]]
          } else {
            buffer = buffer :+ col.asInstanceOf[Column]
          }
        }
        df.select(buffer:_*)
      }
    }


    val asDenseV = udf((v: Vector) => v.toArray)

    testDF.withColumn("user_feature", asDenseV($"user_feature"))
      .select($"mac", $"source", split_elem($"user_feature", 1818))
      .show(1)


    spark.stop()
  }

}
