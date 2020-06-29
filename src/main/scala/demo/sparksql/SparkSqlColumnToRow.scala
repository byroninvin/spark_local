package demo.sparksql

import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.explode
import org.apache.spark.sql.types.{ArrayType, MapType, StringType, StructType}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions._

/**
 * Created by Joe.Kwan on 2020/6/17
 */
object SparkSqlColumnToRow {


  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf()
    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    sparkConf.set("spark.rdd.compress", "true")
    sparkConf.set("hive.metastore.warehouse.dir", "hdfs://coocaadata/apps/hive/warehouse")
    sparkConf.set("spark.sql.warehouse.dir", "hdfs://coocaadata/apps/hive/warehouse")
    sparkConf.set("hive.metastore.uris","thrift://xl.namenode2.coocaa.com:9083")
    val spark = SparkSession.builder()
      .config(sparkConf)
      .appName(s"${this.getClass.getSimpleName} guanyue".filter(!_.equals('$')))
//      .enableHiveSupport()
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val arrayData = Seq(
      Row("James"),
      Row("Michael"),
      Row("Robert"),
      Row("Washington"),
      Row("Jefferson")
    )

    val arraySchema = new StructType()
      .add("name",StringType)

    val df = spark.createDataFrame(spark.sparkContext.parallelize(arrayData),arraySchema)
    df.printSchema()
    df.show(false)

    /**
     * 结果
     *
     * +----------+
     * |name      |
     * +----------+
     * |James     |
     * |Michael   |
     * |Robert    |
     * |Washington|
     * |Jefferson |
     * +----------+
     */

    df.agg(collect_list($"name").alias("test")).show()

    spark.stop()

  }
}
