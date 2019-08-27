package coocaa.test.fpgrowth

import org.apache.spark.mllib.fpm.FPGrowth
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
  * Created by Joe.Kwan on 2018/12/9 14:57.
  */
object FpGrowthMllibTest {


  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession
      .builder()
      .appName(s"${this.getClass.getSimpleName} guanyue".filter(!_.equals('$')))
      .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()

    import spark.implicits._

    val data = List(
      "1, 2, 5",
      "1, 2, 4, 5",
      "1, 2"
    ).toDF("item")

    data.show()
    println(data.count())

    // 转换成rdd形式, 因为要使用MLLIB
    val transactions: RDD[Array[String]] = data.rdd.map {
      s =>
        val str = s.toString().drop(1).dropRight(1)
        str.trim().split(",")
    }


    // 设置最小频繁项比率,最小支持度和数据分区
    val fpg = new FPGrowth().setMinSupport(0.5).setNumPartitions(8)

    // 训练模型
    val model = fpg.run(transactions)


    // 显示频繁项目集
    val freqItemsets = model.freqItemsets.map { itemset =>
      val items = itemset.items.mkString(",")
      val freq = itemset.freq
      (items, freq)
    }.toDF("items", "freq")
//    freqItemsets.show()


    // 根据置信度生成关联规则
    val minConfidence = 0.6

    val Rules = model.generateAssociationRules(minConfidence)
    val df = Rules.map { s =>
      val L = s.antecedent.mkString(",")
      val R = s.consequent.mkString(",")
      val confidence = s.confidence
      (L, R, confidence)
    }.toDF("lef_collect", "right_collect", "confidence")




    df.show()


    spark.stop()
    
  }
}
