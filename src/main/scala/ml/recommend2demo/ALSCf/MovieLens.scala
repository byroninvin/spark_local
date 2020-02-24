package ml.recommend2demo.ALSCf

import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.recommendation.ALS
import org.apache.spark.sql.SparkSession


/**
  * Created by Joe.Kwan on 2018/8/23. 
  */


object MovieLens {
  case class Rating(userId: Int, movieId: Int, rating: Float, timestamp: Long)
  def main(args: Array[String]): Unit = {


    val spark: SparkSession = SparkSession.builder()
      .appName("join_test2")
      .master("local[*]")
      .getOrCreate()

    def parseRating(str: String): Rating = {
      val fields = str.split("::")
      assert(fields.size == 4)
      Rating(fields(0).toInt, fields(1).toInt, fields(2).toFloat, fields(3).toLong)
    }

    import spark.implicits._
    //指定以后从哪里读取数据
    val ratings = spark.read.textFile("F:\\projectJ\\data\\ml-10M100K\\ratings.dat").map(parseRating).toDF()

    val Array(training, test) = ratings.randomSplit(Array(0.8, 0.2))



    // Build the recommendation model using ALS on the training data
    val als = new ALS()
      .setMaxIter(5)
      .setRegParam(0.01)
      .setUserCol("userId")
      .setItemCol("movieId")
      .setRatingCol("rating")
    val model = als.fit(training)

    model.setColdStartStrategy("drop")

    val predictions = model.transform(test)

    val evaluator = new RegressionEvaluator()
      .setMetricName("rmse")
      .setLabelCol("rating")
      .setPredictionCol("prediction")
    val rmse = evaluator.evaluate(predictions)
    println(s"Root-mean-square error = $rmse")

    // Generate top 10 movie recommendations for each user
    val userRecs = model.recommendForAllUsers(10)
    // Generate top 10 user recommendations for each movie
    val movieRecs = model.recommendForAllItems(10)

    // Generate top 10 movie recommendations for a specified set of users
    val users = ratings.select(als.getUserCol).distinct().limit(3)
    val userSubsetRecs = model.recommendForAllItems(10)
    // Generate top 10 user recommendations for a specified set of movies
    val movies = ratings.select(als.getItemCol).distinct().limit(3)
    val movieSubSetRecs = model.recommendForAllItems(10)

    println(userRecs)
    println(movieRecs)
    println(users)
    println(userSubsetRecs)
    println(movies)
    println(movieSubSetRecs)

    spark.stop()
  }

}
