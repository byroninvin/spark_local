package ml.gbdtlr

import org.apache.spark.ml.{PipelineModel, linalg}
import org.apache.spark.mllib.linalg.{DenseVector, Vectors}
import org.apache.spark.mllib.tree.model.GradientBoostedTreesModel
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.Array.range

/**
 * Created by Joe.Kwan on 2020/1/21
 */
class ModelPredicter {


  def loadGbtInfo(spark: SparkSession, path: String):(GradientBoostedTreesModel, Array[Array[Int]]) = {

    // 导入gbt_model
    val gbtModel: GradientBoostedTreesModel = GradientBoostedTreesModel.load(spark.sparkContext, path+"/gbt_model")
    println("gbt model import successfully!!")
    // 导入tree_leaf_array
    val treeLeafArray: Array[Array[Int]] = spark.sparkContext.textFile(path+"/tree_leaf_array").map(_.split(",")).collect().map(_.map(_.toInt))
    println("tree_leaf_array import successfully!!")

    (gbtModel,treeLeafArray)
  }



  def gbdtLrModelPrediction(predictionData: RDD[(String, DenseVector)],
                            gbtModel: GradientBoostedTreesModel,
                            treeLeafArray: Array[Array[Int]],
                            numTrees: Int,
                            pipelineModel: PipelineModel,
                            spark: SparkSession) = {

    // 常规套路
    import spark.implicits._

    val gbt_preprocessor = new GBTPreprocessor
    val num_trees = numTrees

    // 利用已经训练好的gbt模型构建新的特征
    val newFeaturesSet = predictionData.map {
      x => {
        var gbtFeatures = new Array[Double](0)
        for (i <- range(0, num_trees)) {
          val loc = gbt_preprocessor.gbtPredict(gbtModel.trees(i).topNode, x._2)    // 样本点所落叶节点位置
          val leafArray = new Array[Double](gbtModel.trees(i).numNodes /2 + 1)
          leafArray(treeLeafArray(i).indexOf(loc)) = 1
          gbtFeatures = gbtFeatures ++ leafArray
        }
        (x._1, gbtFeatures)
      }
    }
    val gbtFeatureRDD = newFeaturesSet.map(x => (x._1, Vectors.dense(x._2).asML))

    // 生成可以供pipeline_model使用的格式
    val predictionRDD = predictionData.join(gbtFeatureRDD)
      .map(x=> (x._1, x._2._1.asML, x._2._2.toDense)).toDF("string_index", "features1", "features2")

    val trainModelProcess = new ModelProcess
    val predictionRDDUnion = trainModelProcess.featuresAssember(predictionRDD)

    // 将predictionRDDUnion进行pipeline_model的预测
    val (_, _, result) = trainModelProcess.pipelinePredict(predictionRDDUnion, pipelineModel)

    result

  }


}
