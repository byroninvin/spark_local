package ml.logistic_regression

import org.apache.spark.SparkConf
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, explode}

/**
  * Created by Joe.Kwan on 2019-9-29 15:28. 
  */
object SparkMlLRProbDemo {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf()
    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    sparkConf.set("spark.rdd.compress", "true")
    sparkConf.set("hive.metastore.warehouse.dir", "hdfs://coocaadata/apps/hive/warehouse")
    sparkConf.set("spark.sql.warehouse.dir", "hdfs://coocaadata/apps/hive/warehouse")
    val spark = SparkSession.builder()
      .config(sparkConf)
      .appName(s"${this.getClass.getSimpleName} guanyue".filter(!_.equals('$')))
      .enableHiveSupport()
      .master("local[4]")
      .getOrCreate()


    /**
      * 读取用户特征, 从Als中训练出来的
      */
    val didFeatures = spark.sql(
      """
        |SELECT did, case when source in ('yinhe', 'voole') then 'yinhe' else source end as source, vector as did_feature
        |FROM test.vs_user_features_from_als
      """.stripMargin)
        .select("did", "did_feature")
        .repartition(600)

    didFeatures.createOrReplaceTempView("did_features")

    /**
      * 读取vid特征
      */
    val vidFeatures = spark.sql(
      """
        |SELECT video_id as vid, vector as vid_feature
        |FROM test.vs_video_features_from_als
      """.stripMargin)
        .select("vid", "vid_feature")
        .repartition(600)

    vidFeatures.createOrReplaceTempView("vid_feature")

    /**
      * 使用随机抽样的样本数据, 选取2019-09-20
      */
    val samples = spark.sql(s"""SELECT did, vids, label FROM recommend_stage_two.vs_data_sample_randomly WHERE dt='2019-09-20'""".stripMargin)
      .sample(false, 0.0001, 2019L)
      .withColumn("vid", explode(col("vids")))
      .select("did", "vid", "label").repartition(1500, col("did"))

    /**
      * 样本数据关联特征数据
      */

    val features_df = samples.join(didFeatures, Seq("did"), "left_outer").join(vidFeatures, Seq("vid"), "left_outer")
      .filter(col("did_feature").isNotNull)
      .filter(col("vid_feature").isNotNull)
      .select(
        col("did_feature"),
        col("vid_feature"),
        col("label").cast("double")
      )


    //创建一个特征融合器
    val assembler = new VectorAssembler()
      .setInputCols(Array("did_feature", "vid_feature"))
      .setOutputCol("features")


    //对特征进行融合
    val total_features = assembler.transform(features_df).select("label", "features").toDF("label", "features")

    //将数据集分为训练集和验证集
    val splits = total_features.randomSplit(Array(0.9, 0.1), 2019L)
    val trainDF = splits(0).repartition(1400, col("features"))
    val testDF = splits(1)

    //写一个逻辑回归
    val lr = new LogisticRegression()
      .setFeaturesCol("features")
      .setLabelCol("label")
      .setRegParam(0.2)
      .setElasticNetParam(0.3)
      .setStandardization(true)

    //训练模型
    val model = lr.fit(trainDF)


    model.save("F:\\model_saver\\spark_lr\\")


    spark.stop()
  }

}
