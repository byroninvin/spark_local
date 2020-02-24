package util

import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.spark.sql.SparkSession

/**
 * Created by Joe.Kwan on 2019-11-14 18:29. 
 */
object DateTimeUtilFuc {

  //获取前月的第一天
  def getNowMonthStart(): String = {
    var period: String = ""
    var cal: Calendar = Calendar.getInstance();
    var df: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
    cal.add(Calendar.MONTH, -1);
    cal.set(Calendar.DATE, 1); //设置为1号,当前日期既为本月第一天
    period = df.format(cal.getTime())
    period

  }


  //获取前月的最后一天
  def getNowMonthEnd(): String = {
    var period: String = ""
    var cal: Calendar = Calendar.getInstance();
    var df: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
    cal.set(Calendar.DATE, 0)
    period = df.format(cal.getTime())
    period

  }

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


    val beginDate = getNowMonthStart()
    val endDate = getNowMonthEnd()


    println(beginDate, endDate)

    spark.stop()


  }


}
