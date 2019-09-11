package demo.sparksql

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


/**
  * Created by Joe.Kwan on 2019-8-26 16:54. 
  */
object SparkSqlHiveEnableTest {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf()
    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    sparkConf.set("spark.rdd.compress", "true")
    sparkConf.set("hive.metastore.warehouse.dir", "hdfs://coocaadata/apps/hive/warehouse")
    sparkConf.set("spark.sql.warehouse.dir", "hdfs://coocaadata/apps/hive/warehouse")
    val spark = SparkSession.builder()
      .config(sparkConf)
      .appName(s"${this.getClass.getSimpleName} guanyue".filter(!_.equals('$')))
      .enableHiveSupport()
      .master("local[*]")
      .getOrCreate()


    /**
      * 接入指定数据
      */
    val inputVidInfo = spark.sql(s"""SELECT source, video_ids FROM ods.ods_block_recommend""")
      .filter(col("id")==="101" && col("flag")===1 && col("dt")==="2019-09-05")
      .withColumn("source",
        when(col("source")==="o_iqiyi", "yinhe")
          .when(col("source")==="o_tencent", "tencent")
          .when(col("source")==="o_youku", "youku").otherwise("unknown"))   // 如果空为所有源均可?
      .withColumn("vid_without_split", regexp_replace(col("video_ids"), "\\[|\\]", ""))
      .withColumn("vid_without_split", regexp_replace(col("vid_without_split"), "_[A-Za-z]*_", ""))   // 可能是third_id需要join表转换成coocaa_v_id
      .withColumn("split", split(col("vid_without_split"), ","))
      .withColumn("exploded", explode(col("split")))
      .select("source", "exploded")
      .withColumnRenamed("exploded", "vid")


    inputVidInfo.show()


    spark.stop()

  }

}
