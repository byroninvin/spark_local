package demo.sparksql.basic.SparkMlLibTest

//import org.apache.log4j.{Level, Logger}
//import org.apache.spark.mllib.linalg.Vectors
//import org.apache.spark.mllib.regression.{LabeledPoint, LinearRegressionModel, LinearRegressionWithSGD}
//import org.apache.spark.{SparkConf, SparkContext}
//
///**
//  * Created by Joe.Kwan on 2018/8/26.
//  */
//object LinearRegression {
//
//
//  def main(args: Array[String]): Unit = {
//
//
//    // 构建Spark对象
//
//    val conf = new SparkConf().setAppName("LinearRegressionWithSGD").setMaster("local[*]")
//    val sc = new SparkContext(conf)
//    Logger.getRootLogger.setLevel(Level.WARN)
//
//    // 读取样本数据
//    val data_path1 = "F:\\projectJ\\data\\ml_book_data\\lpsa.data"
//    val data = sc.textFile(data_path1)
//    val examples = data.map{line =>
//      val parts = line.split(",")
//      LabeledPoint(parts(0).toDouble, Vectors.dense(parts(1).split(" ").map(_.toDouble)))
//    }.cache()
//    val numExamples = examples.count()
//
//    // 新建线性回归模型,并设置训练参数
//    val numIterations = 100
//    val stepSize = 1
//    val miniBatchFraction = 1.0
//    val model = LinearRegressionWithSGD.train(examples, numIterations, stepSize, miniBatchFraction)
//    model.weights
//    model.intercept
//
//    // 对样本进行测试
//    val prediciton = model.predict(examples.map(_.features))
//    val predictionAndLabel = prediciton.zip(examples.map(_.label))
//    val print_predict = predictionAndLabel.take(20)
//    println("prediction" + "\t" + "label")
//    for (i <- 0 to print_predict.length -1) {
//      println(print_predict(i)._1 + "\t" + print_predict(i)._2)
//    }
//
//    // 计算误差
//
//
//    val loss = predictionAndLabel.map {
//      case (p, l) =>
//        val err = p - l
//        err * errmodel.weights
//    }.reduce(_ + _)
//    val rmse = math.sqrt(loss / numExamples)
//    println(s"Test RMSE = $rmse.")
//
//    // 保存模型
//    val ModelPath = "F:\\projectJ\\data\\model_save_test"
//    model.save(sc, ModelPath)
//    val sameModel = LinearRegressionModel.load(sc, ModelPath)
//
//
//    sc.stop()
//
//  }
//
//}
