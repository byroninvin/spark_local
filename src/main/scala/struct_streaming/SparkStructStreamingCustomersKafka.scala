package struct_streaming

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.{StringType, StructField, StructType}

/**
 * Created by Joe.Kwan on 2020/6/1
 */
object SparkStructStreamingCustomersKafka {

  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder()
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.rdd.compress", "true")
      .config("spark.speculation.interval", "10000ms")
      .config("spark.sql.tungsten.enabled", "true")
      .config("spark.sql.shuffle.partitions", "800")
      .config("hive.metastore.uris", "thrift://xl.namenode2.coocaa.com:9083")
      .config("spark.sql.warehouse.dir", "hdfs://coocaadata/apps/hive/warehouse")
      .config("spark.sql.streaming.checkpointLocation", "hdfs://coocaadata/user/guanyue/checkpoint_test")
      .appName(s"${this.getClass.getSimpleName} guanyue".filter(!_.equals('$')))
      .master("local[*]")
      .enableHiveSupport()
      .getOrCreate()

    import spark.implicits._

    val kafka_config_server = "xl.kafka8.coocaa.com:6667,xl.kafka9.coocaa.com:6667,xl.kafka10.coocaa.com:6667,xl.kafka11.coocaa.com:6667,xl.kafka12.coocaa.com:6667,xl.kafka13.coocaa.com:6667,xl.kafka14.coocaa.com:6667"
    val kafka_topics = "nrt_msg"
    val kafka_group_id = "nrt"
    val kafka_group_id_v2 = "nrt_v2"
    val duration = "300"

    val ratingDS = spark.readStream
        .format("kafka")
//        .format("org.apache.spark.sql.kafka010.KafkaSourceProvider")
        .option("kafka.bootstrap.servers", kafka_config_server)
        .option("auto.offset.reset", "largest")
        .option("subscribe", kafka_topics)
        .load().selectExpr("CAST(value AS STRING)").as[(String)]


    val query = ratingDS.writeStream.outputMode("append").format("console")
        .start


    query.awaitTermination()


    spark.stop()
  }

}
