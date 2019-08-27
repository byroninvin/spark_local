package coocaa.test.fpgrowth

import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.fpm.FPGrowth
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

/**
  * Created by Joe.Kwan on 2018/12/9 16:09. 
  */
object FpGrowthTestDemo {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    val spark: SparkSession = SparkSession
      .builder()
      .appName(s"${this.getClass.getSimpleName} guanyue".filter(!_.equals('$')))
      .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()

    import spark.implicits._
//    val oriDf = spark.read.option("header", true).csv("F:\\projectP\\coocaa\\seqModel\\src\\-test_data.csv")
    val oriDf = spark.read.option("header", true).csv("F:\\projectDB\\Coocaa\\FpGrownth\\for_test_changed.csv")

    /**
      * 将DF处理成fpg可以读取的格式(ml)
      */
    val oriDfSplit = oriDf
        .withColumn("newColumn", split(oriDf("eq"), "[|]")).select("newColumn")

    val trainingSet: DataFrame = oriDfSplit.withColumn("newColumnArray", sort_array(oriDfSplit("newColumn"))).select("newColumnArray")

//    // 前后两个购物车中的元素没有必要再次去重
//    val trainingSet = oriDfSplitArray.dropDuplicates(Seq("newColumnArray")).distinct()

    trainingSet.cache()
    trainingSet.show()


    /**
      * 此部分用于做trainingSet的验证
      */
    val total = trainingSet.count()
    println("总共有:" + total + "条数据")

    /**
      * df transfrom tmp
      */
//    val df = spark.sparkContext.parallelize(Array((1,1),(2,2),(3,3))).toDF("foo","bar")
//    df.show()
//
//    val df2 = df
//      .withColumn("columnArray",array(df("foo").cast("String"),df("bar").cast("String")))
//      .withColumn("litArray",array(lit("foo"),lit("bar")))
//
//    df2.show()


    /**
      * 训练模型
      */
    val fpgrowth = new FPGrowth()
      .setItemsCol("newColumnArray")    //
      .setMinSupport(0.005)      // 设置最小支持度
      .setMinConfidence(0.6)   // 设置最小的置信度

    val model = fpgrowth.fit(trainingSet)

    /**
      * 查看频繁项集
      */
//    model.freqItemsets.show()

    /**
      * 查看关联规则
      */
//    model.associationRules.show()


    /**
      * 关联规则的后端开发
      * freqItemsets生成的 items, freq
      * associationRules生成的 antecedent, consequent, confidence
      */
    val freItemDf = model.freqItemsets
    val assocRuleDf = model.associationRules

    // df_week_supp = df_week_freq.withColumn("support", df_week_freq.freq/total)

    val supportDf = freItemDf.withColumn("support", freItemDf("freq")/total)
    val supportWithAR = assocRuleDf.join(supportDf, supportDf("items")===assocRuleDf("consequent"), joinType = "left").drop("items")

    // df_week_lift = df_week_supp_ar.withColumn("lift", df_week_supp_ar.confidence/df_week_supp_ar.support)
    val liftDf = supportWithAR.withColumn("lift", supportWithAR("confidence")/supportWithAR("support"))


    // 将array格式转换成str
    val freItemDfResult = freItemDf.withColumn("items", concat_ws(",", freItemDf("items")))


    // df_week_lift_new = df_week_lift.withColumn('antecedent', concat_ws(',', 'antecedent')).withColumn('consequent', concat_ws(',', 'consequent'))
    val liftDfResult = liftDf.withColumn("antecedent", concat_ws(",", liftDf("antecedent")))
        .withColumn("consequent", concat_ws(",", liftDf("consequent")))


    freItemDfResult.show()
    liftDfResult.show()


    spark.stop()

  }
}
