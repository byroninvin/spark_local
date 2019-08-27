package coocaa.test.fpgrowth

import org.apache.spark.ml.fpm.FPGrowth
import org.apache.spark.sql.SparkSession

/**
  * Created by Joe.Kwan on 2018/12/9 13:04. 
  */
object FpGrowthMlTest {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession
      .builder()
      .appName(s"${this.getClass.getSimpleName} guanyue".filter(!_.equals('$')))
      .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()


    import spark.implicits._
    val dataset = spark.createDataset(Seq(
      "1 2 5",
      "1 2 3 5",
      "1 2")
    ).map(t => t.split(" ")).toDF("items")

    dataset.show()


    val fpgrowth = new FPGrowth()
      .setItemsCol("items")    //
      .setMinSupport(0.5)      // 设置最小支持度
      .setMinConfidence(0.6)   // 设置最小的置信度

    // 训练模型
    val model = fpgrowth.fit(dataset)

    // 显示频繁项
//    model.freqItemsets.show()

    // 显示整体关联规则
//    model.associationRules.show()

    // 对数据做预测
    model.transform(dataset).show()

    //
    spark.stop()

  }
}
