package demo.sparksql.basic.SparkMlLibTest

import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


/**
  * Created by Joe.Kwan on 2018/8/27.
  */
object TreeModel {

  def main(args: Array[String]): Unit = {

    // 配置Spark
    val conf: SparkConf = new SparkConf().setAppName("tree").setMaster("local[*]")
    val sc = new SparkContext(conf)


    // 读取数据
    val data: RDD[LabeledPoint] = MLUtils.loadLibSVMFile(sc, "F:\\projectJ\\data\\ml_book_data\\sample_libsvm_data.txt")
    val splits = data.randomSplit(Array(0.7, 0.3))
    val (trainingData, testData) = (splits(0), splits(1))


    //
    val numClasses = 2
    val categoricalFeaturesInfo = Map[Int, Int]()
    val impurity = "gini"
    val maxDepth = 5
    val maxBins = 32

    val model = DecisionTree.trainClassifier(trainingData, numClasses, categoricalFeaturesInfo,impurity, maxDepth, maxBins)

    //
    val labelAndPreds = testData.map { point =>
      val prediction = model.predict(point.features)
      (point.label, prediction)
    }

    val print_predict = labelAndPreds.take(20)
    println("label" + "\t" + "prediction")
    for (i <- 0 to print_predict.length -1) {
      println(print_predict(i)._1 + "\t" + print_predict(i)._2)
    }
    val testErr = labelAndPreds.filter(r => r._1 != r._2).count.toDouble / testData.count()
    println("test error = " + testErr)
    println("learned classification tree model:\n" + model.toDebugString)

  }

}
