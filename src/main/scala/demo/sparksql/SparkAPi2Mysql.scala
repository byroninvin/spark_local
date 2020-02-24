package demo.sparksql

import java.util.Properties

import org.apache.spark.sql.SparkSession

/**
 * Created by Joe.Kwan on 2019-11-1 14:24. 
 */
object SparkAPi2Mysql {

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


    /**
     * 方法一
     */
    val csAlbumDF = spark.read.format("jdbc")
      .options(Map(
        "url" -> "jdbc:mysql://192.168.1.57:3307/recommendation",
        "driver" -> "com.mysql.jdbc.Driver",
        "dbtable" -> "album_album_cs_bs",
        "user" -> "bigdata_business",
        "password" -> "jP9YbCxnhp179W")).load()
    //    csAlbumDF.show(false)
    //      .select("album_id")
    //      .createOrReplaceTempView("recom_titlecompare_teleplay")
    println("可以放到精选中的版块为:", csAlbumDF.count())


    /**
     * 方法二
     */
    val ConnProperties = new Properties()
    ConnProperties.put("driver", "com.mysql.jdbc.Driver")
    ConnProperties.put("user", "bigdata_business")
    ConnProperties.put("password", "jP9YbCxnhp179W")
    ConnProperties.put("fetchsize", "10000")

    val testDF = spark.read.jdbc("jdbc:mysql://192.168.1.57:3307/recommendation",
      "(SELECT album_id FROM album_album_cs_bs) t",
      ConnProperties)
    //      .createOrReplaceTempView("mysql_cb_sim")


    testDF.show(800)




    spark.stop()
  }

}
