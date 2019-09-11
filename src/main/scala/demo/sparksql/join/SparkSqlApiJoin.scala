package demo.sparksql.join

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
  * Created by Joe.Kwan on 2019-8-27 14:56. 
  */
object SparkSqlApiJoin {

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

    val leftDF = Seq(
      ("00153", "A", "t", 0.36),
      ("00154", "B", "y", 0.36),
      ("00155", "C", "y", 0.36),
      ("00156", "D", "y", 0.36),
      ("00157", "A", "t", 0.36)
    ).toDF("did", "vid", "source", "rating1")

    val rightDF = Seq(
      ("A", "B", 0.98, "y", "movie"),
      ("B", "A", 0.98, "t", "movie"),
      ("C", "A", 0.99, "t", "movie"),
      ("D", "A", 0.98, "t", "movie"),
      ("A", "C", 1.00, "y", "movie"),
      ("B", "C", 0.99, "y", "movie"),
      ("C", "B", 0.99, "y", "movie"),
      ("D", "B", 0.99, "y", "movie")
    ).toDF("ori_item", "rec_item", "sim", "source", "category")



    val joinExpression = leftDF.col("vid")===rightDF.col("ori_item") && leftDF.col("source")===rightDF.col("source")

    /**
      * +-----+---+------+-------+--------+--------+----+------+--------+
      * |  did|vid|source|rating1|ori_item|rec_item| sim|source|category|
      * +-----+---+------+-------+--------+--------+----+------+--------+
      * |00153|  A|     t|   0.36|    null|    null|null|  null|    null|
      * |00154|  B|     y|   0.36|       B|       C|0.99|     y|   movie|
      * |00155|  C|     y|   0.36|       C|       B|0.99|     y|   movie|
      * |00156|  D|     y|   0.36|       D|       B|0.99|     y|   movie|
      * |00157|  A|     t|   0.36|    null|    null|null|  null|    null|
      * +-----+---+------+-------+--------+--------+----+------+--------+
      */

    leftDF.join(rightDF, joinExpression, "left_outer").drop(rightDF("source"))
      .select("did", "vid", "source", "rec_item", "rating1", "sim").filter(col("rec_item").isNotNull && col("sim").isNotNull)
      .withColumn("score", col("rating1") * col("sim"))
      .groupBy("rec_item").agg(count(col("vid")).alias("count_vid"))
      .show



    spark.stop()
  }

}
