package ml.test

import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.sql.SparkSession

object LessonTest {

  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder()
      .appName("LessonTest01")
      .master("local[*]").getOrCreate()

    val training = spark.createDataFrame(Seq(
      (1.0, Vectors.dense(2.0, 1.0, 0.1)),
      (0.0, Vectors.dense(0.0, 1.0, -1.0)),
      (0.0, Vectors.dense(0.0, 1.3, 1.0)),
      (1.0, Vectors.dense(2.0, 1.2, -0.5))
    )).toDF("label", "features")


//    training.show()


    val lr = new LogisticRegression()
//    println(lr.explainParams())   // 显示所有参数
    /**
      * aggregationDepth: suggested depth for treeAggregate (>= 2) (default: 2)
      * elasticNetParam: the ElasticNet mixing parameter, in range [0, 1]. For alpha = 0, the penalty is an L2 penalty. For alpha = 1, it is an L1 penalty (default: 0.0)
      * family: The name of family which is a description of the label distribution to be used in the model. Supported options: auto, binomial, multinomial. (default: auto)
      * featuresCol: features column name (default: features)
      * fitIntercept: whether to fit an intercept term (default: true)
      * labelCol: label column name (default: label)
      * lowerBoundsOnCoefficients: The lower bounds on coefficients if fitting under bound constrained optimization. (undefined)
      * lowerBoundsOnIntercepts: The lower bounds on intercepts if fitting under bound constrained optimization. (undefined)
      * maxIter: maximum number of iterations (>= 0) (default: 100)
      * predictionCol: prediction column name (default: prediction)
      * probabilityCol: Column name for predicted class conditional probabilities. Note: Not all models output well-calibrated probability estimates! These probabilities should be treated as confidences, not precise probabilities (default: probability)
      * rawPredictionCol: raw prediction (a.k.a. confidence) column name (default: rawPrediction)
      * regParam: regularization parameter (>= 0) (default: 0.0)
      * standardization: whether to standardize the training features before fitting the model (default: true)
      * threshold: threshold in binary classification prediction, in range [0, 1] (default: 0.5)
      * thresholds: Thresholds in multi-class classification to adjust the probability of predicting each class. Array must have length equal to the number of classes, with values > 0 excepting that at most one value may be 0. The class with largest value p/t is predicted, where p is the original probability of that class and t is the class's threshold (undefined)
      * tol: the convergence tolerance for iterative algorithms (>= 0) (default: 1.0E-6)
      * upperBoundsOnCoefficients: The upper bounds on coefficients if fitting under bound constrained optimization. (undefined)
      * upperBoundsOnIntercepts: The upper bounds on intercepts if fitting under bound constrained optimization. (undefined)
      * weightCol: weight column name. If this is not set or empty, we treat all instance weights as 1.0 (undefined)
      */

    // 修改参数
    lr.setMaxIter(10)
      .setRegParam(0.01)

    // 创建一个模型的转换器
    val modelLR = lr.fit(training)

//    println(modelLR.parent.extractParamMap)     //查看当前模型的参数

    // 使用了ParamMap
    val paramMap = ParamMap(lr.maxIter -> 20)
      .put(lr.maxIter -> 30)
      .put(lr.regParam -> 0.1, lr.threshold -> 0.55)

    val parmMap2 = ParamMap(lr.probabilityCol -> "myProbability")
    val paramMapCombined = paramMap ++ parmMap2
    val model2 = lr.fit(training, paramMapCombined)

//    println(model2.parent.extractParamMap)

    // 装备test数据
    val test = spark.createDataFrame(Seq(
      (1.0, Vectors.dense(2.0, 1.0, 0.1)),
      (0.0, Vectors.dense(0.0, 1.0, -1.0)),
      (0.0, Vectors.dense(0.0, 1.3, 1.0))
    )).toDF("label", "features")

//    model2.transform(test).select("label", "features", "probability", "prodiction")
//        .collect()
//        .foreach{
//          case Row(label: Double, features: Vector, probability: Vector, prediciton: Double)=> println(s"($features, $label) -> probability = $probability, prediction = $prediciton)
//        }
//    model2.transform(test).collect.foreach(println)
    model2.transform(test).toDF().show()

  }

}
