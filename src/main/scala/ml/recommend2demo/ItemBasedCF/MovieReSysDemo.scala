package ml.recommend2demo.ItemBasedCF

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}

import scala.util.Try

/**
  * Created by Joe.Kwan on 2019/1/1 17:53. 
  */

case class UserMovieRating(userID: Int, movieID: Int, var rating: Double)


object MovieReSysDemo {


  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
    conf.setAppName("local_vm_scala_test")
    conf.set("spark.master", "local")
//    conf.set("spark.rdd.compress", "true")
//    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    val spark = SparkSession.builder().config(conf).getOrCreate()


    /**
      * 读取数据
      */

    val userMoviesRatingRDD: RDD[UserMovieRating] = spark.sparkContext
      .textFile("F:\\projectP\\Python3WithNoteBook\\Recommender_system\\recommendation_system_codes\\movielens\\ml-100k\\u.data")
      .map(line => {
        val arr = line.split("\t")
        Try(UserMovieRating(arr(0).trim.toInt, arr(1).trim.toInt, arr(2).trim.toDouble))
      })
      .filter(_.isSuccess)
      .map(_.get)


    /**
      * 计算物品相似度矩阵
      */


    val userAvgRatingRDD = userMoviesRatingRDD
        .map(record => (record.userID, record.rating))
        .groupByKey()
        .map {
          case (userID, iter) => {
            // iter中存储的是当前用户的ID对应的所有评分数据
            // a. 求评分记录总数以及评分的总和
            val (total, sum) = iter.foldLeft((0, 0.0))((a, b) => {
              val v1 = a._1 + 1
              val v2 = a._2 + b
              (v1, v2)
            })
            // b.计算平均值并返回
            (userID, 1.0 * sum / total)
          }
        }
    // 2.2 将原始的评分矩阵转换为取均值的评分矩阵
    val removeAvgRatingUserMoviesRatingRDD = userMoviesRatingRDD
        .map(record => (record.userID, record))
        .join(userAvgRatingRDD)
        .map {
          case (_, (record, avgRating)) => {
            record.rating -= avgRating
            (record.movieID, record)
          }
        }
    // 2.3 计算出每个电影的评分人数
    val numberOfRatersPerMoviesRDD = userMoviesRatingRDD
        .map(record => (record.movieID, 1))
        .reduceByKey(_ + _)

    // 2.4 关联获取每个电影的评分人数
    val userMovieRatingNumberOfRatesRDD = removeAvgRatingUserMoviesRatingRDD
        .join(numberOfRatersPerMoviesRDD)
        .map {
          case (_, (record, raters)) => {
            (record.userID, (record, raters))
          }
        }

    // 2.5 计算出每个用户所用评分下, 电影的数据
    val groupedByUserIDRDD = userMovieRatingNumberOfRatesRDD.groupByKey()


    // 2.6 计算电影成对的平均情况
    val moviePairsRDD =groupedByUserIDRDD
        .flatMap{
          case (userID, iter) => {
            // 从iter中获取电影的成对信息
            // a. 将数据进行一个排序操作(按照电影id进行数据排列)
            val sorted = iter
              .toList
              .sortBy(_._1.movieID)
            // b. 双层循环获取计算结果并返回
            sorted.flatMap {
              case (UserMovieRating(_, movieID1, rating1), raters1) => {
                sorted
                  .filter(_._1.movieID > movieID1)
                  .map {
                    case (UserMovieRating(_, movieID2, rating2), raters2) => {
                      // movieID1和movieID2同时出现
                      val key = (movieID1, movieID2)
                      val ratingProduct = rating1 * rating2
                      val movie1RatingSquared = rating1 * rating1
                      val movie2RatingSquared = rating2 * rating2
                      // 返回计算结果
                      (key, (rating1, raters1, rating2, raters2, ratingProduct, movie1RatingSquared, movie2RatingSquared))
                    }
                  }
              }
            }
          }
        }
    import spark.implicits._
    // 2.7 计算电影的整体的一个评分（也就是物品的相似度矩阵）
    val movieSimilarityRDD = moviePairsRDD
      /* 按照(movieID1, movieID2)键值对进行聚合操作*/
      .groupByKey()
      .mapValues(iter => {
        // 计算当前电影组的相似度
        // iter是一个迭代器，内部的数据类型是：(Double, Int, Double, Int, Double, Double, Double)
        // 对于某一个用户来讲， (movie1的用户评分, movie1的总评分人数, movie2的用户评分, movie2的总评分人数, movie1的评分*movie2的评分，movie1的评分^2，movie2的评分^2）
        // a. 合并数据
        val (rating1, numOfRaters1, rating2, numOfRaters2, ratingProduct, rating1Squared, rating2Squared) = iter.foldRight((List[Double](), List[Int](), List[Double](), List[Int](), List[Double](), List[Double](), List[Double]()))((b, a) => {
          (
            b._1 :: a._1,
            b._2 :: a._2,
            b._3 :: a._3,
            b._4 :: a._4,
            b._5 :: a._5,
            b._6 :: a._6,
            b._7 :: a._7
          )
        })
        // b. 开始正式计算相似度
        // b.1 余弦改进公式的计算
        val dotProduct = ratingProduct.sum
        val rating1NormSq = rating1Squared.sum
        val rating2NormSq = rating2Squared.sum
        val adjuestedCosineCorrelation = dotProduct / (math.sqrt(rating1NormSq) * math.sqrt(rating2NormSq))

        // c. 结果输出
        adjuestedCosineCorrelation
      })
      .map {
        case ((movieID1, movieID2), similarity) => {
          (movieID1, movieID2, similarity)
        }
      }.toDF()
//    movieSimilarityRDD.collect.foreach(println)

    movieSimilarityRDD.show()


    spark.stop()
  }

}
