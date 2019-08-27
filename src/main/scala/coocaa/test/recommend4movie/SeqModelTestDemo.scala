package coocaa.test.recommend4movie

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.mllib.feature.Word2Vec
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
  * Created by Joe.Kwan on 2018/12/26 18:30.
  */
object SeqModelTestDemo {


  case class CosinDf(item: String, item_recomm: String, cos_sim: Double)

  def main(args: Array[String]): Unit = {


    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    /**
      * 导入配置文件
      */
    val sparkConf = new SparkConf()

    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    sparkConf.set("spark.rdd.compress", "true")
    sparkConf.set("hive.metastore.warehouse.dir", "hdfs://coocaadata/apps/hive/warehouse")
    sparkConf.set("spark.sql.warehouse.dir", "hdfs://coocaadata/apps/hive/warehouse")
    val spark = SparkSession.builder()
      .config(sparkConf)
      .appName(s"${this.getClass.getSimpleName}")
      .enableHiveSupport()
      .master("local[*]")
      .getOrCreate()


    /**
      * 读入本地数据
      */
    val oriDf = spark.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .option("mode", "DROPMALFORMED")
      .load("F:\\projectP\\coocaa\\seqModel\\src\\test_data.csv")
      .withColumnRenamed("eq", "seq")
      .select("seq")


    /**
      * 文件格式转化
      * transactions 用于训练模型的seq格式
      */
    val transactions: RDD[Seq[String]] = oriDf.rdd.map {
      s =>
        val str = s.toString().drop(1).dropRight(1)
        str.trim().split("[|]").toSeq
    }


    /**
      * 遍历tokenizer
      * 元素为二维数组, flatmap后去重后为独立的tokenizer
      */

    val tokenizerList = oriDf.rdd.map {
      s =>
        val str = s.toString().drop(1).dropRight(1)
        str.trim().split("[|]").toList
    }.flatMap(_.toList).distinct


    println("一共有:", tokenizerList.distinct.count(), "个独立的tokenizer")







    /**
      * 模型训练
      */
    val word2vec = new Word2Vec()
      .setLearningRate(0.0001)
//      .setMaxSentenceLength()
      .setMinCount(5)
      .setNumIterations(6)
      .setNumPartitions(8)
      .setSeed(1)
      .setVectorSize(200)
      .setWindowSize(7)

    val model = word2vec.fit(transactions)



//    for (i <- tokenizerList) {
//      val synonyms = model.findSynonyms(i, 250)
//      val fordf = synonyms.map { line =>
//        val item = i.toString
//        val synonym = line._1.toString
//        val cosineSimilarity = line._2.toDouble
//        CosinDf(item, synonym, cosineSimilarity)
//      }
//    }





    val synonyms = model.findSynonyms("超时空同居", 50)

    for((synonym, cosineSimilarity) <- synonyms) {
      println(s"$synonym $cosineSimilarity")
    }

    spark.stop()


  }

}
