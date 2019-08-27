package machinelearning.test

import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature.{HashingTF, Tokenizer}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.ml.linalg.Vector

object LessonPipeline {


  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder().master("local[*]")
      .appName("PipelineTest")
      .getOrCreate()
    val training: DataFrame = spark.createDataFrame(Seq(
      (0L, "a b c d e spark", 1.0),
      (1L, "b d", 0.0),
      (2L, "spark f g h", 1.0),
      (3L, "hadoop mapreduce", 0.0)
    )).toDF("id", "text", "label")


    // tokenizer组件
    val tokenizer = new Tokenizer()
      .setInputCol("text").setOutputCol("words")

    // hashingTF组件
    val hashingTF = new HashingTF()
      .setInputCol(tokenizer.getOutputCol).setOutputCol("features")
      .setNumFeatures(100)    // 文本量特别大的时候需要指定

    // LR组件
    val lr = new LogisticRegression()
      .setMaxIter(10).setRegParam(0.001)

    // 定义管道
    val pipeline = new Pipeline()
      .setStages(Array(tokenizer, hashingTF, lr))   // 顺序不能错

    val model = pipeline.fit(training)

//    println(tokenizer.explainParams())
//    println(pipeline.explainParams())

    // 保存pipeline
//    pipeline.save("hdfs://192.168.9.11:9000/sparkMLModelSaved/pipelineModelSaved")
    // 保存model
//    model.save("hdfs://192.168.9.11:9000/sparkMLModelSaved/lrModelSaved")

    // 载入pipeline, model
//    val model2 = PipelineModel.load("hdfs://192.168.9.11:9000/sparkMLModelSaved/lrModelSaved")

    // 测试数据
    val test = spark.createDataFrame(Seq(
      (4L, "spark i j k"),
      (5L, "l m n"),
      (6L, "mapreduce spark"),
      (7L, "apache hadoop")
    )).toDF("id", "text")

//    model.transform(test).toDF().show()

    model.transform(test).select("id","text","probability","prediction").collect().foreach{
      case Row(id: Long, text: String, probability: Vector, prediction: Double) =>
        println(s"($text, $id) -> probability = $probability, prediction=$prediction")
    }

    spark.stop()


  }


}
