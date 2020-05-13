package demo.sparksql

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Row, SparkSession, types}
import org.apache.spark.sql.functions.{col, collect_list, concat_ws, sort_array, udf}
import org.apache.spark.sql.types.{ArrayType, IntegerType, StringType, StructField, StructType}

/**
 * Created by Joe.Kwan on 2020/5/8
 */
object SparkSqlCreateEmptyDF {

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

    /**
     * 也可以给每列指定相对应的类型
     */
    val schema1 = StructType(
      Seq(
        StructField("id", IntegerType, true),
        StructField("test", ArrayType(StringType, true), true)))
    val emptyDf1 = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], schema1)
    emptyDf1.show


    spark.stop()
  }

}
