package coocaa.test.fpgrowth

import coocaa.test.common.DateUtil
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.mllib.fpm.FPGrowth
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession

/**
  * Created by Joe.Kwan on 2018/12/11 13:39. 
  */
object FpGrowthMllibCoocaaTest {


  def main(args: Array[String]): Unit = {

    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    val sparkConf = new SparkConf()

    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    sparkConf.set("spark.rdd.compress", "true")
    sparkConf.set("hive.metastore.warehouse.dir", "hdfs://coocaadata/apps/hive/warehouse")
    sparkConf.set("spark.sql.warehouse.dir", "hdfs://coocaadata/apps/hive/warehouse")
    val spark = SparkSession.builder()
      .config(sparkConf)
      .appName(s"${this.getClass.getSimpleName} guanyue".filter(!_.equals('$')))
      .enableHiveSupport()
      .master("local[*]")
      .getOrCreate()

    /**
      * 引入时间模块
      * date_Str: 当前日期的前一天
      * n_day_before: date_Str的前n天
      */
    val date_Str =  DateUtil.getDateBeforeDay
    val n_day_before = DateUtil.getFormatDate(DateUtil.getDateBeforeOrAfter(DateUtil.getDateFromString(date_Str, "yyyy-MM-dd"), -7), "yyyy-MM-dd");


    /**
      * 数据载入
      */
    val oriDf = spark.sql(
      """
        |select mac as mac, seq as seq
        |from (
        |  select mac as mac,concat_ws('|', collect_set(name)) as seq
        |  from(
        |    select mac, `name`
        |    from (
        |      select
        |      upper(mac) as mac,
        |      regexp_replace(`name`, " |,", "") as `name`  --去掉特殊符号
        |      from edw.edw_player
        |      where dur > 300 and
        |      partition_day >= '2018-12-10' and partition_day <= '2018-12-10' and
        |      video_id is not null and
        |      category = '电影'
        |      group by mac,`name` ) name_without_space --tmp_table01
        |    group by mac, `name` ) name_dropduplicates --tmp_table02
        |  group by mac ) b --tmp_table03
        |where length(b.seq) >=10
      """.stripMargin).select("seq")

    oriDf.cache()
    // 参看总共生成的的条数(购物篮,mac数量,用户数量)
    val total = oriDf.count()

    /**
      * 将DF处理成fpg可以读取的格式(mllib)
      */
    val transactions: RDD[Array[String]] = oriDf.rdd.map {
      s =>
        val str = s.toString().drop(1).dropRight(1)
        str.trim().split("[|]")
    }


    /**
      * 训练模型
      * 设置最小频繁项比率,最小支持度和数据分区
      */
    val fpg = new FPGrowth().setMinSupport(0.001).setNumPartitions(8)
    val model = fpg.run(transactions)


    import spark.implicits._
    val freItemDf = model.freqItemsets.map { itemset =>
      val items = itemset.items.mkString(",")
      val freq = itemset.freq
      (items, freq)
    }.toDF("items", "freq")

    /**
      * 生成相对应的关联规则
      */
    val minConfidence = 0.01

    val Rules = model.generateAssociationRules(minConfidence)
    val assocRuleDf = Rules.map { s =>
      val antecedent = s.antecedent.mkString(",")
      val consequent = s.consequent.mkString(",")
      val confidence = s.confidence
      (antecedent, consequent, confidence)
    }.toDF("antecedent", "consequent", "confidence")


    /**
      * 关联规则展示表的开发
      * freqItemsets生成的 items, freq
      * associationRules生成的 antecedent, consequent, confidence
      *
      *
      */
    // 生成fpgrowth的支持度
    val supportDf = freItemDf.withColumn("support", freItemDf("freq")/total)
    val supportWithAR = assocRuleDf.join(supportDf, supportDf("items")===assocRuleDf("consequent"), joinType = "left").drop("items")

    // 生成fpgrowth的提升度
    val liftDf = supportWithAR.withColumn("lift", supportWithAR("confidence")/supportWithAR("support"))


    // 将array格式转换成str
    val freItemDfResult = freItemDf.withColumn("items", concat_ws(",", freItemDf("items")))
      .withColumn("calc_date", current_date())
      .orderBy(desc("freq"))


    // 将array格式转换成str
    val liftDfResult = liftDf.withColumn("antecedent", concat_ws(",", liftDf("antecedent")))
      .withColumn("consequent", concat_ws(",", liftDf("consequent")))
      .withColumn("calc_date", current_date())
      .orderBy(desc("confidence"), desc("support"), desc("lift"))


    /**
      * 查看结果集
      */
    freItemDfResult.show()
    liftDfResult.show()

    println(freItemDfResult.count())
    println(liftDfResult.count())

    spark.stop()


  }
}
