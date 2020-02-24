import org.apache.spark.sql.{Row, SparkSession, types}
import org.apache.spark.sql.types.{DateType, IntegerType, StringType, StructField}
import org.apache.spark.sql.functions._

/**
 * Created by Joe.Kwan on 2020/2/6
 */
object SparkSqlDuplicates {

  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder()
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.rdd.compress", "true")
      .config("spark.speculation.interval", "10000ms")
      .config("spark.sql.tungsten.enabled", "true")
      .config("spark.sql.shuffle.partitions", "800")
//      .config("hive.metastore.uris", "thrift://xl.namenode2.coocaa.com:9083")
//      .config("spark.sql.warehouse.dir", "hdfs://coocaadata/apps/hive/warehouse")
      .appName(s"${this.getClass.getSimpleName} guanyue".filter(!_.equals('$')))
      .master("local[*]")
//      .enableHiveSupport()
      .getOrCreate()


    /**
     * 将Seq序列转换成spark dataframe
     *
     */
    import spark.implicits._
    val df = Seq(
      (1, "tank", 25),
      (2, "zhang", 26),
      (1, "tank", 26)
    ).toDF("id", "name", "age")

//    df.dropDuplicates(Seq("id", "name")).show()
//    println(df.as[String].first())

    df.groupBy("id").agg(sumDistinct(col("name")).alias("how_many_name")).show()



    spark.stop()

  }

}
