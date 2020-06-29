package demo.sparksql

import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.explode
import org.apache.spark.sql.types.{ArrayType, MapType, StringType, StructType}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions._

import scala.collection.mutable.ListBuffer

/**
 * Created by Joe.Kwan on 2020/6/17
 */
object SparkSqlTwoSeqElementJoin {


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

    import spark.implicits._

    val arrayData = Seq(
      Row("James", List("Java","Scala"), List("9.1:Java","2.3:Python","3.5:Cpp")),
      Row("Michael", List("Spark","Java","Python"), List("9.1:Java","4.5:Python")),
      Row("Robert", List("Scala","Java"), List("9.1:Java","4.1:Scala"))
//      Row("Robert", None, List("9.1:Java","4.1:Scala")),
//      Row("Robert", List("Scala","Java"), None)
    )

    val arraySchema = new StructType()
      .add("name",StringType)
      .add("test1", ArrayType(StringType))
      .add("test2", ArrayType(StringType))

    val df = spark.createDataFrame(spark.sparkContext.parallelize(arrayData),arraySchema)
    df.printSchema()
    df.show(false)

    /**
     * 结果
     * +-------+---------------------+----------------------+
     * |name   |test1                |test2                 |
     * +-------+---------------------+----------------------+
     * |James  |[Java, Scala]        |[9.1:Java, 2.3:Python]|
     * |Michael|[Spark, Java, Python]|[9.1:Java, 4.5:Python]|
     * |Robert |[Scala, Java]        |[9.1:Java, 4.1:Scala] |
     * +-------+---------------------+----------------------+
     */

    val joinTwoSeq = (a_seq: Seq[String], b_seq: Seq[String]) => a_seq.intersect(b_seq)

    val mergeSeqJoinSeq = (a_seq: Seq[String], b_seq: Seq[String], filter_rank: Int) => {

      val b_seq_take1 = b_seq.map(_.split(":")(1))

      val joined: Seq[String] = joinTwoSeq(a_seq, b_seq_take1)
      // 取出来匹配到的index
      val indexArray = new ListBuffer[Int]()
      for (each_string <- joined) {
        indexArray += b_seq_take1.indexOf(each_string)
      }
      // 从b_seq中按照index取出多个值
      val selectArray = new ListBuffer[String]()
      for (each_index <- indexArray) {
        selectArray += b_seq(each_index)
      }

      selectArray.take(filter_rank).mkString(",")
    }


    val testUDF = udf(mergeSeqJoinSeq)


    df.withColumn("test3", testUDF($"test1", $"test2", lit(15))).show(false)


    spark.stop()

  }
}
