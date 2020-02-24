package ml.recommend2demo.ALSCf

import java.io.File
import java.util.Random

import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.evaluation.RegressionMetrics
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
/**
  * Created by Joe.Kwan on 2018/11/21 18:04. 
  */


/**
  * 需求
  * 数据 ：
  * MovieLens 电影评分数据
  * 功能 要求 ：
  * 1、找出最受欢迎的的 50 部电影，随机选择 10 部让用户即时评分，并给用户推
  * 荐 50 部电影
  * 算法 要求 ：
  * 1、通过ALS实现推荐模型
  * 2、调优模型参数，通过RMSE指标评估并刷选出最优模型
  * 3、创建基准线，确保最优模型高于基准线
  * 开发 要求 ：
  * 1、通过 Idea 本地开发测试
  * 2、提交到集群模式运行
  */
object ALSwithMllibProjectionEx1 {

  /**
    * 定义用户评分函数
    */
  def elicitateRating(movies: Seq[(Int, String)]) ={
    val prompt = "请输入你对电影的评分(1-5<best>),如果没有看过的话请给0分"
    println(prompt)

    val ratings = movies.flatMap{x =>
      var rating: Option[Rating] = None
      var valid = false
      while(!valid) {
        println(x._2+ " :")
        try{
          val r = Console.readInt()
          if (r>5 || r<0){
            println(prompt)
          } else {
            valid = true
            if (r>0) {
              rating = Some(Rating(0, x._1, r))
            }
          }
        } catch {
          case e:Exception => println(prompt)
        }
      }
      rating match {
        case Some(r) => Iterator(r)
        case None => Iterator.empty
      }
    }
    if (ratings.isEmpty) {
      error("请提供该电影的评分!")
    } else {
      ratings
    }
  }

  /**
    * 定义RMSE计算函数
    */

  def computeRmse(model: MatrixFactorizationModel, data:RDD[Rating])={
    val prediction = model.predict(data.map(x=>(x.user, x.product)))
    // 预测结果
    val predData = prediction.map{
      case Rating(user, product, rating)=> ((user, product),rating)}
    // 测试真实结果
    val testData = data.map{
      case Rating(user, product, rating)=> ((user, product),rating)}
    // (测试结果, 真实结果)
    val dataJoinedValues = predData.join(testData).values
    new RegressionMetrics(dataJoinedValues).rootMeanSquaredError
  }


  /**
    * 主函数
    */
  def main(args: Array[String]): Unit = {
    // 配置设置
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

    if (args.length !=1) {
      print("Usage: movieLensHomeDir")
      sys.exit(1)
    }

    val spark: SparkSession = SparkSession.builder()
      .master("local[*]").appName("ALSMllib_projection")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()

    /**
      * 导入数据模块
      */
    val movieLensHomeDir = args(0)
    val ratings = spark.sparkContext.textFile(new File(movieLensHomeDir, "ratings.dat")
      .toString).map{line =>
      val fields = line.split("::")
      // timestamp, user, product, rating
      (fields(3).toLong % 10, Rating(fields(0).toInt, fields(1).toInt, fields(2).toDouble))
    }

    val movies = spark.sparkContext.textFile(new File(movieLensHomeDir, "movies.dat")
    .toString).map { line =>
      val fields = line.split("::")
      // movieId, movieName
      (fields(0).toInt, fields(1))
    }.collectAsMap()

    val numRatings = ratings.count()
    val numUser = ratings.map(x=>x._2.user).distinct().count()
    val numMovie = ratings.map(_._2.product).distinct().count()

    println("Got " + numRatings + " ratings from " + numUser + " users on " + numMovie + " movies.")

    /**
      * 选取最热的50部电影中的随机0.2让用户去评分
      * 评分的结果存储到myRatingRDD里面
      */
    val topMovies = ratings.map(_._2.product).countByValue().toSeq
      .sortBy(-_._2).take(50).map(_._1)

    val random = new Random(0)
    val selectMovies = topMovies.filter(x=>random.nextDouble() < 0.2)
      .map(x=> (x, movies(x)))

    val myRatings = elicitateRating(selectMovies)
    val myRatingsRDD = spark.sparkContext.parallelize(myRatings, 1)

    /**
      * train_test_split
      */
    val numPartitions = 10
    val trainSet = ratings.filter(x=> x._1< 6).map(_._2).union(myRatingsRDD)      // 增加了新的影评数据
      .repartition(numPartitions).persist()
    val validationSet = ratings.filter(x=>x._1 >=6 && x._1< 8).map(_._2).persist()
    val testSet = ratings.filter(x=> x._1>=8).map(_._2).persist()

    val numTrain = trainSet.count()
    val numValidation = validationSet.count()
    val numTest = testSet.count()

    println("training data: " + numTrain)
    println("validation data: " + numValidation)
    println("test data: " + numTest)

    /**
      * Train model and optimize model with validation set
      */
    val numRanks = List(8, 12)
    val numIters = List(10, 20)
    val numLambdas = List(0.1, 10.0)
    var bestRmse = Double.MaxValue
    var bestModel: Option[MatrixFactorizationModel]=None
    var bestRanks = -1
    var bestIters = 0
    var bestLambdas = -1.0

    for (rank <-numRanks; iter <-numIters; lambda <-numLambdas){
      val model = ALS.train(trainSet, rank, iter, lambda)
      val validationRmse = computeRmse(model, validationSet)
      println("RMSE(validation) = " + validationRmse
        + "with ranks = " + rank
        + ", iter = " + iter
        + ", Lambda = " + lambda)

      if (validationRmse < bestRmse) {
        bestModel = Some(model)
        bestRmse = validationRmse
        bestIters = iter
        bestLambdas = lambda
        bestRanks = rank
      }
    }


    /**
      * Evaluate model on test set
      */
    val testRmse = computeRmse(bestModel.get, testSet)

    println("The best model was trained with rank = " + bestRanks
    + ", Iter = " + bestIters
    + ", Lambda = " + bestLambdas
    + " and compute Rmse on test is " + testRmse)


    /**
      * Create a baseline and compare it with best model
      */
    val meanRating = trainSet.union(validationSet).map(_.rating).mean()
    val bestlineRmse = new RegressionMetrics(testSet.map(x=> (x.rating, meanRating)))
      .rootMeanSquaredError

    val improvement = (bestlineRmse - testRmse) / bestlineRmse * 100
    println("the best model improves the baseline by + " + " %1.2f".format(improvement)+ "%.")


    /**
      * Make a personal recommendation
      */
    val moviesId = myRatings.map(_.product)
    val candidates = spark.sparkContext.parallelize(movies.keys.filter(!moviesId.contains(_)).toSeq)
    val recommendations = bestModel.get
      .predict(candidates.map(x=> (0,x)))
      .sortBy(-_.rating)
      .take(50)

    var i = 0
    println("Movies recommended for you:")
    recommendations.foreach{ line=>
      println("%2d".format(i) + " :" + movies(line.product))
      i += 1
    }
    spark.stop()
  }
}

