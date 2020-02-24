import java.util

import coocaa.test.common.DateUtil
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions.{col, split, to_date}


/**
 * Created by Joe.Kwan on 2020/2/16
 */
object SparkGetPartitionDateList {


  def recommendRelyOnCheckOld(spark: SparkSession, check_date: String): Boolean = {

    val check_date_day_before = DateUtil.getFormatDate(DateUtil.getDateBeforeOrAfter(DateUtil.getDateFromString(check_date, "yyyy-MM-dd"), -1), "yyyy-MM-dd")
    // 当日启播数量
    val check_date_size: Long = spark.sql(s"""select * from edw.edw_player_test where partition_day='$check_date'""").count()
    println(s"check_date_size is $check_date_size")
    // 前一日启播数量
    val check_date_day_before_size: Long = spark.sql(s"""select * from edw.edw_player_test where partition_day='$check_date_day_before'""").count()
    println(s"check_date_day_before_size is $check_date_day_before_size")
    // partner_unmatch_check
    val check_partner_unmatch: Long = spark.sql(s"""select * from edw.edw_player_test where partition_day='$check_date' and partner ='unmatch'""").count()
    println(s"check_partner_unmatch is $check_partner_unmatch")
    // 计算增量
    val incremental_quantity: Double = (check_date_size - check_date_day_before_size).toDouble / check_date_day_before_size
    println(s"incremental_quantity is $incremental_quantity")
    // 计算unmatch占比
    val unmatch_partner_percent: Double = check_partner_unmatch.toDouble/check_date_size
    println(s"unmatch_partner_percent is $unmatch_partner_percent")

    var check_flag = true
    // 当日数据比前一日少多少
    if (check_date_size == 0) {
      check_flag = false
    }  else if (incremental_quantity < -0.6) {
      println(s">>>check_date_incremental_quantity is $incremental_quantity, which is unusual!!!")
      check_flag = false
    } else if (unmatch_partner_percent > 0.3) {
      println(s""">>>unmatch partner percent is $unmatch_partner_percent, which is unusual!!!""")
      check_flag = false
    }
    check_flag
  }




  def getPartitionDaysListFromHive(spark: SparkSession, dbAndTb: String): List[String] = {

    try {

      val partitionsColName = spark.sql(s"""show partitions $dbAndTb""").columns(0)
      spark.sql(s"""show partitions $dbAndTb""")
        .withColumn("splitted", split(col(partitionsColName), "="))
        .select(col("splitted").getItem(1).as("partition_day"))
        .select("partition_day").collect().map(_(0).toString).toList
    }
  }





  def recommendDataPartitionCheck(spark: SparkSession, dbAndTb: String, check_date: String) = {

    // get dbAndTb's MPD
    val tb_partiton_day_list = getPartitionDaysListFromHive(spark, dbAndTb)

    var check_flag = false
    if (tb_partiton_day_list.contains(check_date)) {
      check_flag = true
    }
    check_flag
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

    //
    val dbAndTb = "edw.edw_player_test"
//    val partitionsColName = spark.sql(s"""show partitions $dbAndTb""").columns(0)
//    val test: List[String] = spark.sql(s"""show partitions $dbAndTb""")
//      .withColumn("splitted", split(col(partitionsColName), "="))
//      .select(col("splitted").getItem(1).as("partition_day"))
//      .select("partition_day").collect().map(_(0).toString).toList
//
//    println(test)

    val need_check_date = "2020-02-14"
//
//    val flags = recommendDataPartitionCheck(spark, dbAndTb, need_check_date)
//
//    println(flags)


    val flags2 = recommendRelyOnCheckOld(spark, need_check_date)

    println(flags2)


    spark.stop()


  }

}
