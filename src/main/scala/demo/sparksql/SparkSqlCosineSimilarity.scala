package demo.sparksql

import org.apache.spark.SparkConf
import org.apache.spark.ml.linalg.DenseVector
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

import scala.collection.mutable.ListBuffer

/**
  * Created by Joe.Kwan on 2019-10-23 10:24. 
  */
object SparkSqlCosineSimilarity {


  /**
    *
    * @param vec
    * @return
    */
  def module(vec: DenseVector) ={
    math.sqrt(vec.toArray.map(math.pow(_,2)).sum)
  }

  /**
    *
    * @param v1
    * @param v2
    * @return
    */
  def innerProduct(v1: DenseVector, v2: DenseVector) = {
    val listBuffer = ListBuffer[Double]()
    for (i <-0 until v1.size; j <-0 to v2.size; if i==j) {
      if (i==j) listBuffer.append(v1(i) * v2(j))
    }
    listBuffer.sum
  }


  /**
    *
    * @param v1
    * @param v2
    */
  def cosvec(v1: DenseVector, v2: DenseVector) = {
    val cos = innerProduct(v1, v2) / (module(v1) * module(v2))
    if (cos <=1) cos else 1.0
  }





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




    /**
      * 创建vecotr
      */
    val tableA = spark.sql("""SELECT video_id, vector FROM test.vs_video_features_from_als WHERE video_id='r97b0ls0000'""")
        .select(col("video_id").alias("id01"),col("vector").alias("vector01"))


    val tableB = spark.sql("""SELECT video_id FROM test.vs_video_features_from_als WHERE video_id in ('1fosks5s0000', '57vveqf00000')""")
        .select(col("video_id").alias("id02"),col("vector").alias("vector02"))


    /**
      * 创建一个DF
      */

    val cosineFuction =  spark.udf.register("cosine", cosvec _)

    // when verson<spark2.1
    spark.conf.set("spark.sql.crossJoin.enabled", "true")
    tableA.join(tableB).select("id01", "id02", "vector01", "vector02")
      .withColumn("cos_sim", cosineFuction(col("vector01"), col("vector02")))
      .select("id01", "id02", "cos_sim")
      .show()


//    tableA.crossJoin(tableB).select("vector01", "vector02")
//      .withColumn("cos_sim", cosineFuction(col("vector01"), col("vector02")))
//      .show()






    spark.stop()

  }


}
