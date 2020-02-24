package demo.sparksql

import coocaa.test.common.DateUtil
import org.apache.spark.sql.SparkSession

/**
 * Created by Joe.Kwan on 2019-11-1 14:25. 
 */
object SparkDateTimeUtilTest {


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


    val dateBefore60 = DateUtil.getFormatDate(DateUtil.getDateBeforeOrAfter(DateUtil.getFormatDate("2019-11-01", "yyyy-MM-dd"), -0), "yyyyMMdd")
    println(dateBefore60)


    spark.stop()
  }

}
