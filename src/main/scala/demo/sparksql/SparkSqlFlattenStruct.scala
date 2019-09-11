package demo.sparksql

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


/**
  * Created by Joe.Kwan on 2019-8-26 16:54. 
  */
object SparkSqlFlattenStruct {

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
    val inputVidInfo = spark.sql(s"""SELECT * FROM recommend_stage_two.album_user_player_history_for_album where day='2019-09-10' limit 10""")

//    inputVidInfo.printSchema()

    /**
      * root
      * |-- mac: string (nullable = true)
      * |-- did: string (nullable = true)
      * |-- uid: string (nullable = true)
      * |-- plays: map (nullable = true)
      * |    |-- key: string
      * |    |-- value: struct (valueContainsNull = true)
      * |    |    |-- album_id: string (nullable = true)
      * |    |    |-- album_name: string (nullable = true)
      * |    |    |-- partner: string (nullable = true)
      * |    |    |-- click_hour: long (nullable = true)
      * |    |    |-- first_play_date: string (nullable = true)
      * |    |    |-- last_play_date: string (nullable = true)
      * |    |    |-- click_day_nums: long (nullable = true)
      * |    |    |-- hours_click_nums: long (nullable = true)
      * |-- day: string (nullable = true)
      */
    inputVidInfo.select(explode(col("plays"))).select("value.partner").alias("source").show()

    spark.stop()

  }

}
