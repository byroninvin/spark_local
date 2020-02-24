package ml.recommend2demo.ALSItem.model

import org.apache.spark.sql.SparkSession

object DataPrepareTest {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder
      .master("local[*]")
      .appName("data prepare test")
      .getOrCreate()

    val input = spark.read.option("header", true).csv("F:\\projectP\\coocaa\\offlineRecomm\\src\\testData.csv")

    val trainAndTest = input.randomSplit(Array(0.1, 0.8))
    val trainDataFrame = trainAndTest(0).cache()
//    val testDataRrame = trainAndTest(1).cache()

    val trainRdd = trainDataFrame.rdd.map(row => {
      val uid = row.getString(0)
      val aid = row.getString(1)
      val score = row.getString(2).toDouble
      (uid, (aid, score))
    }).cache()

    val userIndex = trainRdd.map(x => x._1).distinct().zipWithIndex().map(x => (x._1, x._2.toInt)).cache()
    val itemIndex = trainRdd.map(x => x._2._1).distinct().zipWithIndex().map(x => (x._1, x._2.toInt)).cache()


    userIndex.take(10).foreach(println)

    spark.stop()
  }

}
