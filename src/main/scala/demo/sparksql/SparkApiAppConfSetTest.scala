package demo.sparksql

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import com.typesafe.config.ConfigFactory

/**
  * Created by Joe.Kwan on 2019-9-16 16:50. 
  */
object SparkApiAppConfSetTest {

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



    // 加载配置文件 appliaction.conf -> appliaction.json -> application.properties
    val appConfLoad = ConfigFactory.load()

    println(appConfLoad.getString("jdbc.user"))



    spark.stop()
  }

}
