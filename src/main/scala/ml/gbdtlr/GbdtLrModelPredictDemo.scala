package ml.gbdtlr

import org.apache.spark.ml.PipelineModel
import org.apache.spark.mllib.linalg.DenseVector
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.sql.functions.monotonically_increasing_id
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * Created by Joe.Kwan on 2020/1/20
 */
object GbdtLrModelPredictDemo {


  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().master("local[*]")
      .appName("gbdt&lr_model_predict")
      .getOrCreate()

    // 读取数据测试
    // val loadData = spark.sparkContext.textFile("file:///Users/yueguan/Downloads/dataset/gbdtlr_testdata.txt")
//    val loadData = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("file:///Users/yueguan/Downloads/dataset/gbdtlr_testdata.csv")
//
//    loadData.show()
//    import spark.implicits._
//    loadData.rdd.map {
//      row => {
//        val userInfo = row.getAs[String]("user_info")
//        val nonFeatures = row.getAs[String]("non_featrues").split("#").map(_.toDouble)
//        val features = row.getAs[String]("featrues").split("#").map(_.toDouble)
//        val label = row.getAs[Int]("label").toDouble
//
//        (userInfo, LabeledPoint(label, new DenseVector(features)), LabeledPoint(label, new DenseVector(nonFeatures)), (label, new DenseVector(nonFeatures)))
//
//
//      }
//    }.toDF().show()


    /**
     * 准备一份数据
     */
    val predictDemoDataOri = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("file:///Users/yueguan/Downloads/dataset/gbdtlr_testdata.csv")
    val predictDemoDF = predictDemoDataOri.withColumn("string_index", monotonically_increasing_id())
      .select("string_index", "featrues")

    val predictDemoRDD = predictDemoDF.rdd.map{
      row => {
        val string_index = row.getAs[Int]("string_index").toString
        val features = row.getAs[String]("featrues").split("#").map(_.toDouble)
        (string_index, new DenseVector(features))
      }
    }

    /**
     * 首先读取gbdt模型相关
     */
    val modelPredicter = new ModelPredicter
    val gbt_info_path = "/Users/yueguan/Downloads/Models/gbtlr"
    val (gbt_model, treeLeafArray) = modelPredicter.loadGbtInfo(spark, gbt_info_path)

    /**
     * 然后读取pipeline模型相关
     */
    val pipeline_model_path = "/Users/yueguan/Downloads/Models/gbtlr/pipline_model"
    val pipeline_model = PipelineModel.load(pipeline_model_path)

    /**
     * 预测数据需要进行gbdt的特征转换
     */

    val result = modelPredicter.gbdtLrModelPrediction(predictDemoRDD, gbt_model, treeLeafArray,10, pipeline_model, spark)

    result.show()




    spark.stop()

  }

}
