package demo.sparksql

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, split}

/**
 * Created by Joe.Kwan on 2020/4/15
 */
object SparkSqlShowPartitionsHiveTable {


  def getPartitionDaysListFromHive(spark: SparkSession, dbAndTb: String): List[String] = {
    try {
      val partitionsColName = spark.sql(s"""show partitions $dbAndTb""").columns(0)
      spark.sql(s"""show partitions $dbAndTb""")
        .withColumn("splitted", split(col(partitionsColName), "="))
        .select(col("splitted").getItem(1).as("partition_day"))
        .select("partition_day").collect().map(_(0).toString).toList
    }
  }



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


    val a_list = getPartitionDaysListFromHive(spark, "ods.ods_cc_video_2018_partition")

    val new_test = a_list.contains("2019-10-15")

    println(a_list)
    println(new_test)


    spark.stop()
  }




}
