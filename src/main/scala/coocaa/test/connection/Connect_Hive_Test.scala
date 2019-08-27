package coocaa.test.connection

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object Connect_Hive_Test {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
    //args.map(_.split("=")).filter(_.size==2).map(x=>sparkConf.set(x(0),x(1)))
    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    sparkConf.set("spark.rdd.compress", "true")
    sparkConf.set("hive.metastore.warehouse.dir", "hdfs://coocaadata/apps/hive/warehouse")
    sparkConf.set("spark.sql.warehouse.dir", "hdfs://coocaadata/apps/hive/warehouse")
    val spark = SparkSession.builder()
      .config(sparkConf)
      .appName(this.getClass.getSimpleName)
      .enableHiveSupport()
      .master("local[*]")
      .getOrCreate()
    spark.sql("select * from ods.ods_cc_video_source_2018 limit 1").show(false)

   /* spark.sql(
      """
        |select '20181125' as data_date,
        |count(mac) as pv,
        |count(distinct mac) as uv
        |from default.base_clog
        |where productid='CC_Video_6.0'
        |and day='20181125'
        |and  name='search_keyboard_clicked'
      """.stripMargin).show(false)*/
    spark.stop()

  }
}
