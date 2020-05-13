package demo.sparksql

import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
 * Created by Joe.Kwan on 2020/3/18
 */
object SparkSqlArrayOperation {

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
      ("id1", 9.5,"vid01", 0.5),
      ("id1", 9.5,"vid02", 0.4),
      ("id1", 9.5,"vid03", 0.8),
      ("id2", 3.0,"vid02", 0.4),
      ("id2", 3.0,"vid03", 0.5),
      ("id2", 3.0,"vid04", 0.7)).toDF("did", "rating1","vid", "score")


    /**
     * +---+-------+---------------------------------+
     * |did|rating1|vids                             |
     * +---+-------+---------------------------------+
     * |id2|3.0    |[0.7:vid04, 0.5:vid03, 0.4:vid02]|
     * |id1|9.5    |[0.8:vid03, 0.5:vid01, 0.4:vid02]|
     * +---+-------+---------------------------------+
     */
    val mergeList = udf{(str: Seq[String]) => str.map(_.split(":")(1)).toList.mkString(",")}

    val mergeListTest2 = (aSeq: Seq[String], rating1:Double) => {
      val newSeq = new ListBuffer[String]()
      aSeq.map { row=>
        val sim = row.split(":")(0).toDouble
        val vid = row.split(":")(1)
        val oneElement = (sim * rating1).formatted("%.2f")+":"+vid
        newSeq += oneElement
      }
      newSeq
    }
    val mergeListTest2UDF = udf(mergeListTest2)


    val middleTb = data.withColumn("connect", concat_ws(":", col("score"), col("vid")))
      .groupBy("did", "rating1").agg(sort_array(collect_list("connect"), asc = false).alias("vids"))
      .withColumn("test", mergeListTest2UDF(col("vids"), col("rating1")))

    middleTb.show(false)

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



//    val mergeList = udf{(str: Seq[String]) => str.map(_.split(":")(1)).toList.mkString(",")}
//
//    data.withColumn("connect", concat_ws(":", col("score"), col("vid")))
//        .groupBy("did").agg(sort_array(collect_list("connect"), asc = false).alias("vids"))
//        .withColumn("vids", mergeList(col("vids")))
//        .show(false)





    spark.stop()
  }

}
