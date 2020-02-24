import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.apache.spark.sql.types.{ArrayType, DoubleType, FloatType, IntegerType, LongType, StringType, StructField, StructType}

/**
 * Created by Joe.Kwan on 2020/2/3
 */
object SparkTFRecords {

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

    val path = "file:///Users/yueguan/Downloads/test/spark2tfrecords/test-output.tfrecord"
    val testRows: Array[Row] = Array(
      new GenericRow(Array[Any](11, 1, 23L, 10.0F, 14.0, List(1.0, 2.0), "r1")),
      new GenericRow(Array[Any](21, 1, 24L, 12.1F, 15.0, List(1.0, 3.0), "r2"))
    )

    val schema = StructType(List(
      StructField("id", IntegerType),
      StructField("IntegerCol", IntegerType),
      StructField("LongCol", LongType),
      StructField("FloatCol", FloatType),
      StructField("DoubleCol", DoubleType),
      StructField("VectorCol", ArrayType(DoubleType, true)),
      StructField("StringCol", StringType)
    ))

    val rdd = spark.sparkContext.parallelize(testRows)

    val df = spark.createDataFrame(rdd, schema)

    // save dataFrame
//    df.write.format("tfrecords").option("recordType", "Example").save(path)
//    println("model dump done!")

    // read tfrecords into dataframe
    // the dataframe schema is inferred from TFRecords if no custom schema is provided
    val importedDF1 = spark.read.format("tfrecords").option("recordType", "Example").load(path)
    importedDF1.show()

    // read TFRecords into DataFrame using custom schema
    val importedDF2 = spark.read.format("tfrecords").schema(schema).load(path)
    importedDF2.show()

    df.show()

    spark.stop()

  }

}
