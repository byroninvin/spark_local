package ml.gbdtlr

import org.apache.spark.mllib.linalg.DenseVector
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import ml.gbdtlr.GBTPreprocessor
import org.apache.spark.mllib.linalg
import org.apache.spark.rdd.RDD

/**
 * Created by Joe.Kwan on 2020/1/25
 */
object GbdtLrModelTrainProcess {

  def main(args: Array[String]): Unit = {


    val spark: SparkSession = SparkSession.builder().master("local[*]")
      .appName("PipelineTest")
      .getOrCreate()

    // 读取数据测试
    val loadData = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("file:///Users/yueguan/Downloads/dataset/gbdtlr_testdata.csv")

    val loadDataWithIndex = loadData.withColumn("string_index", monotonically_increasing_id())
      .select("string_index", "featrues", "label")


    loadData.schema
    val trainingData = loadDataWithIndex.rdd.map {
      row =>
        val features = row.getAs[String]("featrues").split("#").map(_.toDouble)
        val string_index= row.getAs[Long]("string_index").toString
        val label = row.getAs[Int]("label").toDouble
        (string_index, LabeledPoint(label, new DenseVector(features)), (label, new DenseVector(features)))
    }

    trainingData.foreach(println)

    val testData = trainingData

    /**
     * 实例化一个model process
     *
     */
    val trainModelProcess = new ModelProcess()

    val (trainDF, testDF, gbt_model, tree_leaf_array) = trainModelProcess.gbtFeatureProcess(trainingData, testData, spark)

    trainDF.show()
    testDF.show()

    val unionTrainDF = trainModelProcess.featuresAssember(trainDF)
    val unionTestDF = trainModelProcess.featuresAssember(testDF)

    // 管道训练和预测
    val pipeline_model = trainModelProcess.pipelineTrain(unionTrainDF)
    val (scalerDF, selectedDF, lrPredictions) = trainModelProcess.pipelinePredict(unionTestDF, pipeline_model)

    scalerDF.show()
    selectedDF.show()
    lrPredictions.show()

    /**
     * 存储相关的模型
     */

    // 1.存储gbdt相关
    val gbtPreprocessor = new GBTPreprocessor()
    val gbt_info_path = "/Users/yueguan/Downloads/Models/gbtlr"
    gbtPreprocessor.dumpGbtInfo(gbt_model, tree_leaf_array, 10, path=gbt_info_path, spark)
    // 2.存储pipeline模型
    val pipeline_model_path = "/Users/yueguan/Downloads/Models/gbtlr/pipline_model"
    pipeline_model.write.overwrite().save(pipeline_model_path)


    println("gbdt and lr training process is done!!")
    spark.stop()
  }

}
