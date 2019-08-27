package machinelearning.recommend2demo.ALSCf

import org.apache.spark.ml.recommendation.ALS
import org.apache.spark.sql.SparkSession


/**
  * Created by Joe.Kwan on 2018/11/5 14:51. 
  */
object ALSwithMl {

  case class Movie(movieId:Int, title:String, genres:Seq[String])
  case class User(userId:Int, gender:String, age:Int, occupation:Int, zip:String)
  case class Rating(user: Int, product: Int, rating: Double)

  // Define parse function
  def parseMovie(str: String):Movie = {
    val fields = str.split("::")
    assert(fields.size == 3)
    Movie(fields(0).toInt, fields(1).toString, Seq(fields(2)))
  }

  def parseUser(str: String): User = {
    val fields = str.split("::")
    assert(fields.size == 5)
    User(fields(0).toInt, fields(1).toString, fields(2).toInt, fields(3).toInt,
      fields(4).toString)
  }

  def parseRating(str: String): Rating = {
    val fields = str.split("::")
    assert(fields.size == 4)
    Rating(fields(0).toInt, fields(1).toInt, fields(2).toFloat)
  }

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .appName("MovieLensALSTest")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._
    val ratings = spark.read.textFile("F:\\projectP\\Python3WithNoteBook\\Recommender_system\\recommendation_system_codes\\movielens\\ml-1m\\ratings.dat")
      .map(parseRating).cache()
    println("total number of ratings: " + ratings.count())
    println("total number of movies rated: " + ratings.map(_.product).distinct().count())
    println("total numbser of users who rate movies: " + ratings.map(_.user).distinct().count())

    // Create DataFrames
    val ratingDF = ratings.toDF()
    val movieDF = spark.read.textFile("F:\\projectP\\Python3WithNoteBook\\Recommender_system\\recommendation_system_codes\\movielens\\ml-1m\\movies.dat")
      .map(parseMovie).toDF()
    val userDF = spark.read.textFile("F:\\projectP\\Python3WithNoteBook\\Recommender_system\\recommendation_system_codes\\movielens\\ml-1m\\users.dat")
      .map(parseUser).toDF()

    ratingDF.printSchema()
    movieDF.printSchema()
    userDF.printSchema()
    ratingDF.createOrReplaceTempView("ratings")
    movieDF.createOrReplaceTempView("movies")
    userDF.createOrReplaceTempView("users")

    val result=spark.sql(
      """
        |select title, rmax, rmin, ucnt
        |from
        |(select product, max(rating) as rmax, min(rating) as rmin, count(distinct user) as ucnt from ratings group by product) ratingsCNT
        |join movies on product=movieId
        |order by ucnt desc
      """.stripMargin)
    result.show()

    // ALS_ML
    val splits = ratings.randomSplit(Array(0.8, 0.2), 0L)
    val trainingSet = splits(0).cache()
    val testSet = splits(1).cache()
    println(trainingSet.count())
    println(testSet.count())

    val als = new ALS()
      .setMaxIter(5)
      .setRegParam(0.01)
      .setUserCol("userId")
      .setItemCol("movieId")
      .setRatingCol("rating")

    val model = als.fit(trainingSet)
    model.setColdStartStrategy("drop")
    val predictions = model.transform(trainingSet)





    spark.stop()

  }

}
