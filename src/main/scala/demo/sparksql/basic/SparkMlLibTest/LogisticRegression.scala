package demo.sparksql.basic.SparkMlLibTest

import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.classification.{LogisticRegressionModel, LogisticRegressionWithLBFGS}
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}



/**
  * Created by Joe.Kwan on 2018/8/27. 
  */
object LogisticRegression {

  def main(args: Array[String]): Unit = {

    // 构建spark对象
    val conf: SparkConf = new SparkConf().setAppName("logistic_regression").setMaster("local[*]")
    val sc = new SparkContext(conf)
    Logger.getRootLogger.setLevel(Level.WARN)

    // 读取样本数据1, 格式为LIBSVM format
    val dataPath = "F:\\projectJ\\data\\ml_book_data\\sample_libsvm_data.txt"
    val data: RDD[LabeledPoint] = MLUtils.loadLibSVMFile(sc, dataPath)
//    data.foreach(x => println(x))

    // 样本数据划分训练样本与测试样本
    val splits: Array[RDD[LabeledPoint]] = data.randomSplit(Array(0.6, 0.4), seed = 11L)
    val training: RDD[LabeledPoint] = splits(0).cache()
    val test: RDD[LabeledPoint] = splits(1)

    // 新建逻辑回归模型,并训练
    val model: LogisticRegressionModel = new LogisticRegressionWithLBFGS().setNumClasses(10).run(training)
    model.weights
    model.intercept


    // 对测试样本进行测试
    val predictionAndLabel: RDD[(Double, Double)] = test.map {
      case LabeledPoint(label, features) =>
        val prediction = model.predict(features)

        (prediction, label)
    }
    val print_predict: Array[(Double, Double)] = predictionAndLabel.take(20)
    println("prediction" + "\t" + "label")

    for (i <- 0 to print_predict.length - 1) {
      println(print_predict(i)._1 + "\t" + print_predict(i)._2)
    }

    // 计算误差
    val metrics = new MulticlassMetrics(predictionAndLabel)
    val precision: Any = metrics.accuracy
    println("accuracy = " + precision)

    val ModelSavePath = "F:\\projectJ\\data\\model_save_test2"
    model.save(sc, ModelSavePath)
    val sameModel: LogisticRegressionModel = LogisticRegressionModel.load(sc,ModelSavePath)


    sc.stop()

  }


}
