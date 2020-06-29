package timetest

import java.util.{Calendar, Date}

import coocaa.test.common.DateUtil
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.udf

/**
 * Created by Joe.Kwan on 2020/6/17
 */
object SparkGetCurrentHour {

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
      .enableHiveSupport()
      .getOrCreate()

    val day="20200530"

    val day_two = "20200201"

    /**
     * 将Seq序列转换成spark dataframe
     *
     */
//    import spark.implicits._
//    val df = Seq(
//      (1, "tank", "2020-05-30", "20200721"),
//      (2, "zhang", "2018-02-01", "20190101")
//    ).toDF("id", "name", "date_one", "date_two")
//
//    val dateConvertUdf = udf{ (day: String) => DateUtil.getDateStr(DateUtil.getFormatDate(day, "yyyyMMdd"), "yyyy-MM-dd")}
//
//    df.withColumn("date_three", dateConvertUdf($"date_two")).show()

    val currentDateTime: Date = DateUtil.getCurrDate()

    println(currentDateTime.getHours)
    val calendar: Calendar = Calendar.getInstance()
    println(calendar.get(Calendar.HOUR_OF_DAY))


    val myHour = DateUtil.getHourToday()
    println(myHour)


    spark.stop()
  }

}
