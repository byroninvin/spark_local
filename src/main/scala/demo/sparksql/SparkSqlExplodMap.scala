package demo.sparksql

import org.apache.spark.SparkConf
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions.{col, explode}
import org.apache.spark.sql.types.{ArrayType, MapType, StringType, StructType}

/**
 * Created by Joe.Kwan on 2020/6/17
 */
object SparkSqlExplodMap {


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
      .enableHiveSupport()
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val arrayData = Seq(
      Row("James",List("Java","Scala"),Map("hair"->"black","eye"->"brown")),
      Row("Michael",List("Spark","Java",null),Map("hair"->"brown","eye"->null)),
      Row("Robert",List("CSharp",""),Map("hair"->"red","eye"->"")),
      Row("Washington",null,null),
      Row("Jefferson",List(),Map())
    )

    val arraySchema = new StructType()
      .add("name",StringType)
      .add("knownLanguages", ArrayType(StringType))
      .add("properties", MapType(StringType,StringType))

    val df = spark.createDataFrame(spark.sparkContext.parallelize(arrayData),arraySchema)
    df.printSchema()
    df.show(false)

    /**
     * 结果
     * +----------+-------------------+--------------------------------+
     * |name      |knownLanguages     |properties                      |
     * +----------+-------------------+--------------------------------+
     * |James     |[Java, Scala]      |Map(hair -> black, eye -> brown)|
     * |Michael   |[Spark, Java, null]|Map(hair -> brown, eye -> null) |
     * |Robert    |[CSharp, ]         |Map(hair -> red, eye -> )       |
     * |Washington|null               |null                            |
     * |Jefferson |[]                 |Map()                           |
     * +----------+-------------------+--------------------------------+
     */


    df.select($"name",explode($"properties"))
      .show(false)


    spark.stop()

  }
}
