package demo.sparksql.basic.SparkMlLibTest

import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.recommendation.Rating
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.recommendation.ALS


/**
  * Created by Joe.Kwan on 2018/8/27. 
  */
object ALSModel {

  def main(args: Array[String]): Unit = {

    // 构建Spark对象
    val conf: SparkConf = new SparkConf().setAppName("ALS").setMaster("local[*]")
    val sc = new SparkContext(conf)
    Logger.getRootLogger.setLevel(Level.WARN)

    // 读取样本数据
    val data: RDD[String] = sc.textFile("F:\\projectJ\\data\\ml_book_data\\test.data")
    val ratings: RDD[Rating] = data.map(_.split(",") match {
      case Array(user, item, rate) =>
        Rating(user.toInt, item.toInt, rate.toDouble)
    })

    // 创建模型
    val rank = 10
    val numIterations = 20
    val model = ALS.train(ratings, rank, numIterations, 0.01)

    // 预测结果
    val usersProducts: RDD[(Int, Int)] = ratings.map {
      case Rating(user, product, rate) =>
        (user, product)
    }


    val predictions: RDD[((Int, Int), Double)] = ratings.map {
      case Rating(user, product, rate) =>
        ((user, product), rate)
    }

    val ratesAndPreds: RDD[((Int, Int), (Double, Double))] = ratings.map {
      case Rating(user, product, rate) =>
        ((user, product), rate)
    }.join(predictions)


    val MSE: Any = ratesAndPreds.map {
      case ((user, product), (r1, r2)) =>
        val err = (r1 - r2)
        err * err
    }.mean()

    println("Mean Squared Error = " + MSE)



  }

}
