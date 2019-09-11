package demo.sparksql

import org.apache.spark.SparkConf
import org.apache.spark.sql.{AnalysisException, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._


/**
  * Created by Joe.Kwan on 2019-8-26 16:54. 
  */
object SparkSqlHiveEnable {

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


    def getMaxPartitionDaysFromHive(dbAndTb: String): String = {

      try {
        spark.sql(s"""show partitions $dbAndTb""")
          .withColumn("splitted", split(col("partition"), "="))
          .select(col("splitted").getItem(1).as("partition_day"))
          .withColumn("partition_day", to_date(col("partition_day")))
          .withColumn("partition_day", max(col("partition_day"))).collect()(0)(0).toString

      } catch {
        case ex: Exception => "Table is not a partition table"
      }

    }

//    val test = getMaxPartitionDaysFromHive("recommend_stage_two.vs_recall_seqmodel_all")
//
//    println(test)

    /**
      * 查看df的列名
      */
    val myTest = spark.sql("""show partitions recommend_stage_two.vs_recall_itembasedcf_all""").columns(0)
    println(myTest)


    spark.stop()

  }

}
