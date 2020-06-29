package ml

import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.Row
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.ml.linalg.{SparseVector => MLSV}
import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.Aggregator
import util.{Breeze2SparkConverter, Spark2BreezeConverter}
//import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.sql.{Encoder, Encoders}
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.mllib.stat.MultivariateOnlineSummarizer

/**
 * Created by Joe.Kwan on 2020/5/11
 */
object SparkSparseVectorAggregation {


//  type Summarizer = MultivariateOnlineSummarizer
//
//  case class VectorSumarizer(f: String) extends Aggregator[Row, Summarizer, Vector]
//    with Serializable {
//    def zero = new Summarizer
//    def reduce(acc: Summarizer, x: Row) = acc.add(x.getAs[Vector](f))
//    def merge(acc1: Summarizer, acc2: Summarizer) = acc1.merge(acc2)
//
//    // This can be easily generalized to support additional statistics
//    def finish(acc: Summarizer) = acc.mean
//
//    def bufferEncoder: Encoder[Summarizer] = Encoders.kryo[Summarizer]
//    def outputEncoder: Encoder[Vector] = ExpressionEncoder()
//  }


  def main(args: Array[String]): Unit = {

    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
    val spark: SparkSession = SparkSession
      .builder()
      .appName(s"${this.getClass.getSimpleName} guanyue".filter(!_.equals('$')))
      .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .enableHiveSupport()
      .getOrCreate()
    spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")
    // 设置orc解析模式 如果分区下没有文件也能在sql也能查询不会抛错
    spark.sqlContext.setConf("spark.sql.hive.convertMetastoreOrc", "false")


    import spark.implicits._

    def dv(values: Double*) = Vectors.dense(values.toArray).toSparse

    val df = spark.createDataFrame(Seq(
      (1, dv(0,0,5)), (1, dv(4,0,1)), (1, dv(1,2,1)),
      (2, dv(7,5,0)), (2, dv(3,3,4)),
      (3, dv(0,8,1)), (3, dv(0,0,1)), (3, dv(7,7,7)))
    ).toDF("id", "vec")


    def spark_sparse_vector_add(vector_one: MLSV, vector_two: MLSV):MLSV = {
      require(vector_one.size == vector_two.size)
      val breeze_mac_feature = Spark2BreezeConverter.sparseSpark2SparseBreezeConverter.convert(vector_one)
      val breeze_vid_feature = Spark2BreezeConverter.sparseSpark2SparseBreezeConverter.convert(vector_two)
      val merge_feature = breeze_mac_feature + breeze_vid_feature
      Breeze2SparkConverter.sparseBreeze2SparseSparkVector.convert(merge_feature)
    }

    df.show()


    df.rdd.map { row =>
      val id = row.getAs[Int]("id")
      val feature = row.getAs[MLSV]("vec")
      (id, (feature, 1))
    }

      .reduceByKey((a, b) => (spark_sparse_vector_add(a._1, b._1), a._2 + b._2))
      .map {row =>
      val scal_number: Double = 1.0 / row._2._2
        util.OriBLAS.scal(scal_number, row._2._1)
        (row._1, row._2._1)
    }.toDF("id", "mean_vec").show()

//
//
//    aggregated.show()







    spark.stop()
  }

}
