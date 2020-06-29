//package spark_streaming
//
//import kafka.serializer.StringDecoder
//import org.apache.spark.sql.SparkSession
//import org.apache.spark.streaming.{Seconds, StreamingContext}
//import org.slf4j.LoggerFactory
//import util.ValueUtils
//import org.apache.spark.streaming.kafka.KafkaUtils
//
///**
// * Created by Joe.Kwan on 2020/6/1
// */
//object SparkStreamingCustomersKafka {
//
//
//  private final val logger = LoggerFactory.getILoggerFactory.getLogger(this.getClass.getSimpleName)
//
//  case class kafkaData(mac: String, block_id: String, partner: String, istvb: Int, threetime: String)
//
//  def main(args: Array[String]): Unit = {
//
//    val spark: SparkSession = SparkSession.builder()
//      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
//      .config("spark.rdd.compress", "true")
//      .config("spark.speculation.interval", "10000ms")
//      .config("spark.sql.tungsten.enabled", "true")
//      .config("spark.sql.shuffle.partitions", "800")
//      .config("hive.metastore.uris", "thrift://xl.namenode2.coocaa.com:9083")
//      .config("spark.sql.warehouse.dir", "hdfs://coocaadata/apps/hive/warehouse")
//      .config("spark.sql.streaming.checkpointLocation", "hdfs://coocaadata/user/guanyue/checkpoint_test")
//      .appName(s"${this.getClass.getSimpleName} guanyue".filter(!_.equals('$')))
//      .master("local[*]")
//      .enableHiveSupport()
//      .getOrCreate()
//
//    import spark.implicits._
//
//    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
//
//    ssc.sparkContext.setLogLevel("WARN")
//
//    // 准备3.kafka
//    val topics = "nrt_msg"
//    val topicsSet = topics.split(",").toSet
//    logger.warn("消费的topics is: " + topicsSet.toList)
//    // TODO: 这里还是需要改的！！！！，调资源的时候改
//    val kafkaParams = Map[String, String](
//      "metadata.broker.list" -> ValueUtils.getStringValue("nrt.metadata.broker.list"),
//      "auto.offset.reset" -> "largest",
//      "group.id" -> "vloggroup"
//    )
//
//    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicsSet)
//
//
//
//
//
//    spark.stop()
//  }
//
//}
