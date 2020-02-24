import org.apache.calcite.avatica.ColumnMetaData.StructType
import org.apache.spark.sql.{DataFrame, Row, SparkSession, types}
import org.apache.spark.sql.types.{DateType, IntegerType, StringType, StructField, StructType}

/**
 * Created by Joe.Kwan on 2019-10-30 14:38. 
 */
object SparkCreateDataSet {

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
     * 将Seq序列转换成spark dataframe
     *
     */
    import spark.implicits._
    val df = Seq(
      (1, "tank", 25),
      (2, "zhang", 26)
    ).toDF("id", "name", "age")


    /**
     * 将列表转换成dataframe
     */

    val array = List(
      (1, "tank", 25),
      (2, "zhang", 26),
      (3, "li", 28)
    )

    val df1 = array.toDF("id", "name", "age")


    /**
     * 通过createDataFrame创建dataframe
     */

    val schema = types.StructType(List(
      StructField("id", IntegerType, nullable = false),
      StructField("name", StringType, nullable = true),
      StructField("age", IntegerType, nullable = true),
      StructField("birthday", DateType, nullable = true)
    ))


    val rdd = spark.sparkContext.parallelize(Seq(
      Row(1, "tank1", 25, java.sql.Date.valueOf("2019-10-27")),
      Row(2, "zhang", 23, java.sql.Date.valueOf("2018-10-27"))
    ))

    val df2 = spark.createDataFrame(rdd, schema)

//    df2.join()
    df.show()
    df1.show()
    df2.show()


    spark.stop()

  }

}
