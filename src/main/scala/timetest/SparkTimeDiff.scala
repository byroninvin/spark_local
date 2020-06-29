package timetest

import coocaa.test.common.DateUtil
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
 * Created by Joe.Kwan on 2020/6/12
 */
object SparkTimeDiff {

  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder()
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.rdd.compress", "true")
      .config("spark.speculation.interval", "10000ms")
      .config("spark.sql.tungsten.enabled", "true")
      .config("spark.sql.shuffle.partitions", "800")
      .config("hive.metastore.uris", "thrift://xl.namenode2.coocaa.com:9083")
      .config("spark.sql.warehouse.dir", "hdfs://coocaadata/apps/hive/warehouse")
      .appName(s"${this.getClass.getSimpleName} guanyue".filter(!_.equals('$')))
      .master("local[*]")
//      .enableHiveSupport()
      .getOrCreate()

    val day="20200530"

    val day_two = "20200201"

    /**
     * 将Seq序列转换成spark dataframe
     *
     */
    import spark.implicits._
    val df = Seq(
      (1, "tank", "2000-05-30", "2020-07-21"),
      (2, "zhang", "2008-02-01", "2019-01-01")
    ).toDF("id", "name", "date_one", "date_two")

    val dateConvertUdf = udf{ (day: String) => DateUtil.getDateStr(DateUtil.getFormatDate(day, "yyyyMMdd"), "yyyy-MM-dd")}



    df.withColumn("test", round(-datediff($"date_one", $"date_two")/365))
      .withColumn("test2", pow(lit(0.98), $"test")).show()


//    df.withColumn("month_diff", datediff($"date_two", $"date_one") / 30)
//      .withColumn("round_month_diff", round($"month_diff")).show()



    spark.stop()
  }

}
