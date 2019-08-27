package coocaa.test.df2mysql

import java.util.Properties

import org.apache.spark.SparkConf
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

object Write_Test {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName(s"${this.getClass.getSimpleName} guanyue".filter(!_.equals('$')))
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.set("spark.rdd.compress", "true")
    conf.set("spark.speculation.interval", "10000ms")
    conf.set("spark.sql.tungsten.enabled", "true")
    conf.set("spark.sql.shuffle.partitions", "200")
    conf.set("spark.Kryoserializer.buffer.max", "1024m")
    conf.set("spark.sql.warehouse.dir", "hdfs://coocaadata/apps/hive/warehouse")
    conf.set("spark.sql.broadcastTimeout","5400")

    //set "spark.sql.broadcastTimeout" to increase the timeout

    // 创建sparksession
    val spark = SparkSession.builder().config(conf).master("local[*]").enableHiveSupport().getOrCreate()




    val arr = Array("4 zhangyi 34","5 huagnying 32","6 yamei 30")

    val dataRDD = spark.sparkContext.parallelize(arr).map(_.split(" "))
    val schema=StructType(
      List(
        StructField("id",IntegerType,true),
        StructField("name",StringType,true),
        StructField("age",IntegerType,true)
      )
    )
    val rowRDD = dataRDD.map(x=>Row(x(0).toInt,x(1),x(2).toInt))
    // val studentDF = sqlContext.createDataFrame(rowRDD,schema)
    val studentDF = spark.sqlContext.createDataFrame(rowRDD, schema)


    val url = "jdbc:mysql://192.168.1.57:3307/test?useUnicode=true&characterEncoding=UTF-8"

    val props = new Properties()
    props.put("user","hadoop2")
    props.put("password","pw.mDFL1ap")
    props.put("driver", "com.mysql.jdbc.Driver")
    studentDF.write.mode("Overwrite").jdbc(url,"usr_player_day_video_info",props)
    spark.stop()

  }
}
