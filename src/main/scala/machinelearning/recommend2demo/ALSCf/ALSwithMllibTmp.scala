package machinelearning.recommend2demo.ALSCf

import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.recommendation.{ALS, Rating}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession


/**
  * Created by Joe.Kwan on 2018/11/21 16:51.
  */

object ALSwithMllibTmp {


  case class Movie(movieId:Int, title:String, genres:Seq[String])
  def parseMovie(str: String): Movie = {
    val fields = str.split("::")
    assert(fields.size == 3)
    Movie(fields(0).toInt, fields(1).toString, Seq(fields(2)))
  }
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
    val spark: SparkSession = SparkSession.builder()
      .master("local[*]").appName("ALStest")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()

    /**
      * 电影Info数据
      */
    import spark.implicits._
    val movieDF = spark.sparkContext.textFile("F:\\projectP\\Python3WithNoteBook\\Recommender_system\\recommendation_system_codes\\movielens\\ml-1m\\movies.dat").map(parseMovie).toDF()


    val lines: RDD[String] = spark.sparkContext.textFile("F:\\projectP\\Python3WithNoteBook\\Recommender_system\\recommendation_system_codes\\movielens\\ml-1m\\ratings.dat")

    /**
      * 使用mllib.recommendation.Rating的正确方法
      */
    val ratings = lines.map(_.split("::").take(3) match {
      case Array(user, item, rate) => Rating(user.toInt, item.toInt, rate.toDouble)
    }).cache()
    //    println(ratings.first())

    /**
      * 划分训练数据与测试数据
      */
    val splits = ratings.randomSplit(Array(0.8, 0.2), 0L)
    val trainingSet = splits(0).cache()
    val testSet = splits(1).cache()

    /**
      * 创建ALS模型
      * 基于ALS（alternating least squares）的协同过滤算法，涉及参数如下：
      * numBlocks: 计算并行度(若为-1表示自动化配置)
      * Rank：模型中隐含影响因子，默认是10
      * Iterations：迭代次数，默认是10
      * Lambda：ALS中正则化参数
      * implicitPrefs：是否使用显式反馈变量或使用隐式反馈数据的变量
      * Alpha：ALS中的一个参数，作用于隐式反馈变量，控制基本的信心度
      */
    val model = ALS.train(trainingSet, rank = 20, iterations = 10, lambda = 0.01)


    /**
      * 为一个user推荐topN个item
      */
//    val recomForTopUser = model.recommendProducts(4169, 100)
//    val movieTitle = movieDF.rdd.map(array => (array(0), array(1))).collectAsMap()
//    recomForTopUser.map(rating => (movieTitle(rating.product), rating.rating)).foreach(println)

    /**
      * 为(user product)预测评分,
      * 用于计算testSet的mae结果
      */
    val testUserProduct = testSet.map{
      case Rating(user, product, rating) => (user, product)
    }
    val testUserProductPredict = model.predict(testUserProduct)
//    println(testUserProductPredict.take(10).mkString("\n"))   //  查看前10个(user, product)预测的结果


    val testSetPair=testSet.map{
      case Rating(user, product, rating) => ((user, product), rating)
    }
    val predictionsPair=testUserProductPredict.map{
      case Rating(user, product, rating) => ((user, product), rating)
    }

    val joinTestPredict = testSetPair.join(predictionsPair)
    // 计算mae
    val mae = joinTestPredict.map{
      case((user,product), (ratingT, ratingP)) =>
        val err= ratingT-ratingP
        Math.abs(err)
    }.mean()
//    println("模型的绝对值误差为: ",mae)    //  打印出mae结果
    // 计算FP
    val fp = joinTestPredict.filter{
      case ((user, product), (ratingT, ratingP)) =>
        (ratingT <=1 & ratingP>=4)
    }
    println("模型满足FP结果的个数", fp.count())   //  打印fp结果值


    /**
      * 使用org.apache.spark.mllib.evaluation._的api来评测模型
      */
    import org.apache.spark.mllib.evaluation._
    val ratingTP = joinTestPredict.map{
      case((user, product), (ratingT, ratingP)) =>
        (ratingP, ratingT)      // (预测结果, 测试使用的正确结果)
    }
    val evalutor = new RegressionMetrics(ratingTP)
    println("模型的均方误差为: ", evalutor.meanAbsoluteError)
    println("模型的均方根误差为: ", evalutor.rootMeanSquaredError)








    spark.stop()
  }


}
