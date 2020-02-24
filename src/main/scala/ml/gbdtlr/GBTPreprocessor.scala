package ml.gbdtlr

import org.apache.spark.mllib.linalg.{DenseVector, Vectors}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.GradientBoostedTrees
import org.apache.spark.mllib.tree.configuration.{BoostingStrategy, FeatureType}
import org.apache.spark.mllib.tree.model.{GradientBoostedTreesModel, Node}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.Array.range



/**
 * Created by Joe.Kwan on 2020/1/20
 */
class GBTPreprocessor extends Serializable {

  /**
   *
   * @param node
   * @return
   */
  def getLeafNode(node: Node): Array[Int] = {
    var leafNode = Array[Int]()
    if (node.isLeaf) {
      leafNode = leafNode :+ node.id
    }
    else {
      leafNode = leafNode ++ getLeafNode(node.leftNode.get)
      leafNode = leafNode ++ getLeafNode(node.rightNode.get)
    }
    leafNode
  }

  /**
   *
   * @param node
   * @param features
   * @return
   */
  def gbtPredict(node: Node, features: DenseVector): Int = {
    // 树模型在非叶子节点的分割点
    val split = node.split

    if (node.isLeaf) {
      node.id
    }
    else {
      // 判断连续或者离散，不同的特征判断方式
      if (split.get.featureType == FeatureType.Continuous) {
        // split.get.feature数据类型为int，表示feature index
        if (features(split.get.feature) <= split.get.threshold) {
          gbtPredict(node.leftNode.get, features)
        }
        else {
          gbtPredict(node.rightNode.get, features)
        }
      }
      else {
        // categories默认为左子节点特征列表
        if (split.get.categories.contains(features(split.get.feature))) {
          gbtPredict(node.leftNode.get, features)
        }
        else {
          gbtPredict(node.rightNode.get, features)
        }
      }
    }
  }

  /**
   *
   * @param gbtTrainData
   * @param numTrees
   * @return
   */
  def gbtTrain(gbtTrainData: RDD[LabeledPoint], numTrees: Int): (GradientBoostedTreesModel, Array[Array[Int]]) = {

    val boostingStrategy = BoostingStrategy.defaultParams("Classification") //分类模型
    boostingStrategy.setNumIterations(numTrees) // 设置决策树个数

    val gbtModel = GradientBoostedTrees.train(gbtTrainData, boostingStrategy) // gbt模型
    val treeLeafArray = new Array[Array[Int]](numTrees) // 统计各个书中节点node.id在各个树中的分布，括号中一个数则代表行数

    // 存储叶子节点的index
    for (i <- range(0, numTrees)) {
      treeLeafArray(i) = getLeafNode(gbtModel.trees(i).topNode)
    }
    (gbtModel, treeLeafArray)
  }


  /**
   *
   * @param gbtTestData
   * @param gbtModel
   * @param treeLeafArray
   * @param numTrees
   * @return
   */
  def gbtFeaturePredict(gbtTestData: RDD[(String, (Double, DenseVector))],
                        gbtModel: GradientBoostedTreesModel,
                        treeLeafArray: Array[Array[Int]],
                        numTrees: Int): RDD[(String, LabeledPoint)] = {
    // 利用gbt构建新的特征
    val newFeaturesSet = gbtTestData.map {
      x => {
        var gbtFeatures = new Array[Double](0) // 存储新的特征的数组，选择Double类型应为labeledPoint函数要求double类型
        for (i <- range(0, numTrees)) {
          val loc = gbtPredict(gbtModel.trees(i).topNode, x._2._2) // 样本点所落也节点位置
          val leafArray = new Array[Double](gbtModel.trees(i).numNodes / 2 + 1) // 满员二叉树叶节点数=（总节点数/2）+1
          leafArray(treeLeafArray(i).indexOf(loc)) = 1 // val数组元素可变，长度不变
          gbtFeatures = gbtFeatures ++ leafArray // 两边数据类型要求一致
        }
        (x._1, x._2._1, gbtFeatures)
      }
    }
    // 新特征数据
    val gbtFeatureRDD = newFeaturesSet.map(x => (x._1, LabeledPoint(x._2, Vectors.dense(x._3))))

    gbtFeatureRDD
  }


  /**
   * 将训练好的gbtModel保存
   * @param gbtModel
   * @param treeLeafArray
   * @param numTrees
   * @param path
   */
  def dumpGbtInfo(gbtModel: GradientBoostedTreesModel,
                  treeLeafArray: Array[Array[Int]],
                  numTrees: Int,
                  path: String,
                  spark: SparkSession) = {

    // 存储
    gbtModel.save(spark.sparkContext, path+"/gbt_model")
    println(s"gbt_model saved in the path: $path/gbt_model successfully!!")

    // 存储treeLeafArray
    spark.sparkContext.parallelize(treeLeafArray).map(_.mkString(",")).coalesce(1).saveAsTextFile(path+"/tree_leaf_array")
    println(s"treeLeafArray saved in the path: $path/tree_leaf_array successfully!!")

    // numTrees不需要存储
    println(s"num of trees is ${numTrees} and no need to save!!")

  }

}