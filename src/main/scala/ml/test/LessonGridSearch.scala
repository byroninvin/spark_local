package ml.test

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.feature.{HashingTF, Tokenizer}
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object LessonGridSearch {

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().master("local[*]")
      .appName("PipelineTest")
      .getOrCreate()
    val training: DataFrame = spark.createDataFrame(Seq(
      (0L, "a b c d e spark", 1.0),
      (1L, "b d", 0.0),
      (2L, "spark f g h", 1.0),
      (3L, "hadoop mapreduce", 0.0),
      (4L, "b spark who", 1.0),
      (5L, "g d a y", 0.0),
      (6L, "spark fly", 1.0),
      (7L, "was mapreduce", 0.0),
      (8L, "e spark program", 1.0),
      (9L, "a e c l", 0.0),
      (10L, "spark compile", 1.0),
      (11L, "hadoop software", 0.0)
    )).toDF("id", "text", "label")


    // tokenizer组件
    val tokenizer = new Tokenizer()
      .setInputCol("text").setOutputCol("words")

    // hashingTF组件
    val hashingTF = new HashingTF()
      .setInputCol(tokenizer.getOutputCol).setOutputCol("features")

    // LR组件,调优正则化参数
    val lr = new LogisticRegression()
      .setMaxIter(10)

    // 定义参数网格
    val paramGrid = new ParamGridBuilder()
      .addGrid(hashingTF.numFeatures, Array(10, 100, 1000))
      .addGrid(lr.regParam, Array(0.1, 0.01))
      .build()    // 调用一个最优的


    // 定义管道
    val pipeline = new Pipeline()
      .setStages(Array(tokenizer, hashingTF, lr))   // 顺序不能错


    // CV
    val cv = new CrossValidator()
      .setEstimator(pipeline)
      .setEstimatorParamMaps(paramGrid)
      .setEvaluator(new BinaryClassificationEvaluator())
      .setNumFolds(2)    // 实例上会选择一个比较大于3的参数

    // fit
    val cvModel = cv.fit(training)

    // cvModel.explainParams()  // 查看参数

    // test
    val test = spark.createDataFrame(Seq(
      (12L, "spark i j k"),
      (13L, "l m n"),
      (14L, "mapreduce spark"),
      (15L, "apache hadoop")
    )).toDF("id", "text")

    //    model.transform(test).toDF().show()

    cvModel.transform(test).select("id","text","probability","prediction").collect().foreach{
      case Row(id: Long, text: String, probability: Vector, prediction: Double) =>
        println(s"($text, $id) -> probability = $probability, prediction=$prediction")
    }

    spark.stop()


  }



}
