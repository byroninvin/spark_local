package ml.gbdtlr

import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.classification.{LogisticRegression, LogisticRegressionModel}
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.feature._
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.mllib.linalg.DenseVector
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.model.GradientBoostedTreesModel
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}


/**
 * Created by Joe.Kwan on 2020/1/20
 */
class ModelProcess {

  /**
   * 为了更方便的获取GradientBoostedTrees的中间结果，所以使用mllib版本的
   * @param train 训练数据rdd
   * @param test 测试数据rdd
   * @param spark
   * @return （trainDF, testDF, gbtModel, treeLeafArray)
   */
  def gbtFeatureProcess(train:RDD[(String, LabeledPoint, (Double, DenseVector))],
                        test:RDD[(String, LabeledPoint, (Double, DenseVector))],
                        spark: SparkSession): (DataFrame, DataFrame, GradientBoostedTreesModel, Array[Array[Int]]) = {

    import spark.implicits._

    val gbtProcessor = new GBTPreprocessor
    val numTrees = 10
    val (gbtModel, treeLeafArray) = gbtProcessor.gbtTrain(train.map(x=>x._2), numTrees)     // gbt训练

    val gbtTrainRDD = gbtProcessor.gbtFeaturePredict(train.map(x=> (x._1, x._3)), gbtModel, treeLeafArray, numTrees)
      .map(x => ((x._1, x._2.label), x._2.features.asML))

    val allTrainRDD = train.map(x => ((x._1, x._2.label), x._2.features.asML)).join(gbtTrainRDD)
    val trainDF = allTrainRDD.map(x => (x._1._1,x._1._2,x._2._1,x._2._2)).toDF("string_index", "label", "features1","features2")

    //
    val gbtTestRDD = gbtProcessor.gbtFeaturePredict(test.map( x=> (x._1, x._3)), gbtModel, treeLeafArray, numTrees)
      .map(x=> ((x._1, x._2.label), x._2.features.asML))

    val allTestRDD = test.map(x => ((x._1, x._2.label), x._2.features.asML)).join(gbtTestRDD)
    val testDF = allTestRDD.map(x => (x._1._1, x._1._2, x._2._1, x._2._2)).toDF("string_index", "label", "features1", "features2")

    // gbttModel, treeLeafArray 需要存储，线上预测的时候需要使用
    (trainDF, testDF, gbtModel, treeLeafArray)
  }


  /**
   *
   * @param data
   * @return
   */
  def pipelineTrain(data: DataFrame): PipelineModel = {
    data.persist()
    // stage1 scaler model
    val scaler = new MinMaxScaler()
      .setInputCol("features")
      .setOutputCol("scaledFeatures")

    // stage2 卡方特征检验选择
    val chiSqSelector = new ChiSqSelector()
      .setFeaturesCol("scaledFeatures")
      .setLabelCol("label")
      .setNumTopFeatures(80)
      .setOutputCol("selectedFeatures")

    // stage3 lr model
    val lr = new LogisticRegression()
      .setMaxIter(200)
      .setElasticNetParam(1.0)      // 1.0为L1正则化
      .setRegParam(0.00075)
      .setLabelCol("label")
      .setFeaturesCol("selectedFeatures")

    // 建立管道
    val pipeline = new Pipeline()
      .setStages(Array(scaler, chiSqSelector, lr))

    // 建立网络搜索，超参数调优
    val paramGrid = new ParamGridBuilder()
      .addGrid(chiSqSelector.numTopFeatures, Array(70, 80, 90))
      .addGrid(lr.maxIter, Array(100, 150, 200))
      .addGrid(lr.elasticNetParam, Array(1.0, 0.75, 0.5, 0.25, 0.0))
      .addGrid(lr.regParam, Array(0.001, 0.00075, 0.00125))
      .build()

    // 建立evaluator，必须要保证验证的标签列是向量化后的标签
    val evaluator = new BinaryClassificationEvaluator()
      .setLabelCol("label")

    // 建立一个交叉验证的评估器，设置评估器的参数
    val cv = new CrossValidator()
      .setEstimator(pipeline)
      .setEvaluator(evaluator)
      .setEstimatorParamMaps(paramGrid)
      .setNumFolds(2)     // 2折交叉

    /**
     * 运行交叉验证的评估器，设置评估器的参数
     *
     */
    val cvModel = cv.fit(data)
    val piplineModel = cvModel.bestModel.asInstanceOf[PipelineModel]

    data.unpersist()
    piplineModel

  }


  /**
   *
   * @param data
   * @param pipelineModel
   * @return
   */
  def pipelinePredict(data: DataFrame, pipelineModel: PipelineModel): (DataFrame, DataFrame, DataFrame) = {

    data.persist()
    // 管道训练后的三个模型
    val scalerModel = pipelineModel.stages(0).asInstanceOf[MinMaxScalerModel]
    val chiSqSelectorModel = pipelineModel.stages(1).asInstanceOf[ChiSqSelectorModel]
    val lrModel = pipelineModel.stages(2).asInstanceOf[LogisticRegressionModel]

    println("best features selection numbers =>", chiSqSelectorModel.explainParam(chiSqSelectorModel.numTopFeatures))
    println("best lr model max iter numbers =>", lrModel.explainParam(lrModel.maxIter))
    println("best lr reg param =>", lrModel.explainParam(lrModel.regParam))
    println("best lr threshold =>", lrModel.explainParam(lrModel.threshold))
    println("best lr elasticNetParam =>:", lrModel.explainParam(lrModel.elasticNetParam))
    println("best lr num of features =>:", lrModel.numFeatures)

    val scalerData = scalerModel.transform(data)
    val selectedData = chiSqSelectorModel.transform(scalerData)
    val predictions = lrModel.transform(selectedData)

    data.unpersist()
    (scalerData, selectedData, predictions)
  }


  /**
   *
   * @param data
   * @return
   */
  def featuresAssember(data: DataFrame):DataFrame ={
    val assembler = new VectorAssembler()
      .setInputCols(Array("features1", "features2"))
      .setOutputCol("features")

    val output = assembler.transform(data)
    output
  }


  /**
   *
   * @param data
   * @return
   */
  def multiClassEvaluate(data: RDD[(Double, Double)]): (Double, Double, Double, Double) ={
    val metric = new MulticlassMetrics(data)
    val accuracy = metric.accuracy
    val weightedPrecision = metric.weightedPrecision
    val weightedRecall = metric.weightedRecall
    val f1 = metric.weightedFMeasure
    (accuracy, weightedPrecision, weightedRecall, f1)
  }

}
