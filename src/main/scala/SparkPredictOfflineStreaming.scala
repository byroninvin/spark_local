import org.apache.hadoop.fs.Path
import org.apache.spark.sql.streaming.{OutputMode, ProcessingTime, StreamingQuery, Trigger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.functions._

/**
 * Created by Joe.Kwan on 2020/5/20
 */
object SparkPredictOfflineStreaming {


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

    val model_path = "/user/guanyue/test_data/data"
    val path = new Path(model_path)
    val hadoopConf = spark.sparkContext.hadoopConfiguration
    val hdfs = org.apache.hadoop.fs.FileSystem.get(hadoopConf)

    hdfs.delete(path, true)
    println(s"子文件目录: $path, 已经成功删除")

    import org.apache.spark.sql.streaming.StreamingQueryListener
    val myQueryListener = new StreamingQueryListener {
      import org.apache.spark.sql.streaming.StreamingQueryListener._
      def onQueryTerminated(event: QueryTerminatedEvent): Unit = {
        println(s"Query ${event.id} terminated")
      }

      def onQueryStarted(event: QueryStartedEvent): Unit = {}
      def onQueryProgress(event: QueryProgressEvent): Unit = {}
    }

    spark.streams.addListener(myQueryListener)

//    val schema = StructType(
//      Array(
//        StructField("transactionId", StringType),
//        StructField("customerId", StringType),
//        StructField("itemId", StringType),
//        StructField("amountPaid", StringType)
//      )
//    )

    val schema = StructType(
      Array(
        StructField("seq", StringType)
      )
    )

    // create stream from folder
    val fileStreamDf: DataFrame = spark.readStream.option("codec", "org.apache.hadoop.io.compress.SnappyCodec").schema(schema)
      .parquet("/user/guanyue/recommendation/dataset/item2vector/zy_item2vector")

    val count_test = fileStreamDf.withColumn("test_column", lit("666"))

    // val query: StreamingQuery =
//    val query: StreamingQuery = count_test.writeStream
//      .outputMode("complete")
//      .format("parquet")
//      .option("path", "/user/guanyue/test_data/data")
////      .trigger(ProcessingTime("1 seconds"))
//      .trigger(Trigger.Once())
//      .outputMode(OutputMode.Append()).start()

    val q4s = count_test.writeStream.format("console").trigger(Trigger.ProcessingTime("4 seconds"))
        .option("truncate", false).start






//    query.recentProgress
    q4s.awaitTermination(100000)
//    println("执行完成！")
//    query.stop()
//    println("查询结束！")
    spark.stop()
  }
}
