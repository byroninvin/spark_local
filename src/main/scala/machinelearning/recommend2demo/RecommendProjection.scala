package machinelearning.recommend2demo

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.recommendation.{MatrixFactorizationModel, Rating, ALS}
import org.apache.spark.rdd.RDD



/**
  * Created by Joe.Kwan on 2018/8/23. 
  */
object RecommendProjection {

  def main(args: Array[String]): Unit = {

    SetLogger
    val (ratings, movieTitle) = PrepareData()
    val model = ALS.train(ratings, 5, 20, 0.1)
    recommend(model, movieTitle)

  }


  def SetLogger = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("com").setLevel(Level.OFF)
    System.setProperty("spark.ui.showConsoleProgress","false")
    Logger.getRootLogger().setLevel(Level.OFF)
  }



  def PrepareData():(RDD[Rating], Map[Int, String]) = {

    val sc = new SparkContext(new SparkConf().setAppName("Recommend").setMaster("local[*]"))
    print("begin load user rating data...")
    val rawUserData = sc.textFile("F:\\projectJ\\data\\ml-10M100K\\ratings.dat")
    val rawRatings = rawUserData.map(_.split("::").take(3))
    val ratingsRDD = rawRatings.map {
      case Array(user, movie, rating) =>
        Rating(user.toInt, movie.toInt, rating.toDouble)
    }
    println("total: " + ratingsRDD.count.toString() + "pieces ratings")

    print("begin loading movie data")
    val itemRDD = sc.textFile("F:\\projectJ\\data\\ml-10M100K\\movies.dat")
    val movieTitle = itemRDD.map(
      line => line.split("::").take(2)).map(array => (array(0).toInt, array(1))).collect().toMap

    val numRatings = ratingsRDD.count()
    val numUsers = ratingsRDD.map(_.user).distinct().count()
    val numMovies = ratingsRDD.map(_.product).distinct().count()

    println("total: ratings:" + numRatings, "USER " + numRatings + "MOVIES " + numMovies)

    return (ratingsRDD, movieTitle)

  }

  def RecommendMovies(model: MatrixFactorizationModel, movieTitle: Map[Int, String], inputUserID: Int) = {

    val RecommendMovie = model.recommendProducts(inputUserID, 10)
    var i = 1
    println("针对用户id" + inputUserID + "推荐下列电影:")
    RecommendMovie.foreach {
      r => println(i.toString() + "." + movieTitle(r.product) + "评分: " + r.rating.toString())
        i += 1
    }
  }

  def RecommendUsers(model: MatrixFactorizationModel, movieTitle: Map[Int, String], inputMoiveID: Int) ={

    val RecommendUser = model.recommendUsers(inputMoiveID, num = 10)
    var i = 1
    println("针对电影id : " + inputMoiveID + " 推荐下列用户id:")
    RecommendUser.foreach{
      r => println(i.toString + "用户id: " + r.user + " 评分: " + r.rating)
        i = i+ 1
    }
  }

  def recommend(model:MatrixFactorizationModel,movieTitle: Map[Int, String]) = {

    var choose = ""
    while (choose != "3") {
      print("please choose 1 2 3")
      choose = readLine()
      if (choose == "1") {
        print("please input userId")
        val inputUserID = readLine()
        RecommendMovies(model, movieTitle, inputUserID.toInt)
      } else if (choose == "2") {
        print("please input movieId")
        val inputMovieID = readLine()
        RecommendUsers(model, movieTitle, inputMovieID.toInt)
      }
    }
  }


}
