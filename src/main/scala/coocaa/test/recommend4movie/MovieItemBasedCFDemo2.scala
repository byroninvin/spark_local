package coocaa.test.recommend4movie

import java.util.Properties

import coocaa.test.recommend4movie.CFSimilarityMeasures._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.current_date

import scala.util.Try

/**
  * Created by Joe.Kwan on 2019/1/8 18:32.
  * 增加多种相似度计算的demo
  */

case class UserMovieRating2(userID: Int, movieID: Int, var rating: Double)


object MovieItemBasedCFDemo2 {

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
      .appName(s"${this.getClass.getSimpleName} guanyue".filter(!_.equals('$')))
      .enableHiveSupport()
      .master("local[2]")
      .getOrCreate()

    import spark.implicits._
    spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")


    /**
      * 导入hive数据
      */

//    val oriDF = spark.sql(
//      """
//        |select did, video_id, dur as rating
//        |from recommendation.rating_data_v2
//        |limit 10
//      """.stripMargin)
//    oriDF.createOrReplaceTempView("based_table")


    val oriDF = spark.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load("F:\\projectJ\\data\\ml_itemSimSparkData\\ratings.csv")
      .withColumnRenamed("userId", "did")
      .withColumnRenamed("movieId", "video_id")
      .select("did", "video_id", "rating")
      .selectExpr("cast(did as String) did",
        "cast(video_id as String) video_id",
        "cast(rating as Double) rating")

    oriDF.createOrReplaceTempView("based_table")



    //
    val oriData = oriDF.rdd.map { x => (x.getAs[String]("did"), x.getAs[String]("video_id"), x.getAs[Double]("rating")) }.cache()


    // 将user_index映射成int
    val user_index = oriData.map(x => x._1).distinct().zipWithIndex().map(x => (x._1, x._2.toLong))
      .toDF("did", "userID").createOrReplaceTempView("tb_did_index")
    val item_index = oriData.map(x => x._2).distinct().zipWithIndex().map(x => (x._1, x._2.toLong))
      .toDF("video_id", "movieID").createOrReplaceTempView("tb_vid_index")

    oriData.unpersist()
    val userMovieRaingDF = spark.sql(
      """
        |select b.userID as userID, c.movieID as movieID, a.rating as rating
        |from based_table a
        |join tb_did_index b
        |on a.did = b.did
        |join tb_vid_index c
        |on a.video_id = c.video_id
      """.stripMargin).cache()

    // 解放内存
    oriDF.unpersist()


    /**
      * 准备计算, 整理好的数据为 userMovieRaingDF
      * 转换成, userMoviesRatingRDD==> RDD[UserMovieRating]
      */
    val userMoviesRatingRDD: RDD[UserMovieRating] = userMovieRaingDF.rdd.map(x=> {
      val userID = x.getAs[Long]("userID")
      val movieID = x.getAs[Long]("movieID")
      val rating = x.getAs[Double]("rating")
      Try(UserMovieRating(userID.toInt, movieID.toInt, rating.toDouble))
    }).filter(_.isSuccess).map(_.get)


    /**
      * 计算物品相似度矩阵
      */
    val userAvgRatingRDD = userMoviesRatingRDD
      .map(record => (record.userID, record.rating))
      .groupByKey()
      .map {
        case (userID, iter) => {
          // iter中存储的是当前用户的ID对应的所有评分数据
          // 求评分记录总数以及评分的总和
          val (total, sum) = iter.foldLeft((0, 0.0))((a, b) => {
            val v1 = a._1 + 1
            val v2 = a._2 + b
            (v1, v2)
          })
          // 计算平均值并返回
          (userID, 1.0 * sum / total)
        }
      }
    // 将原始的评分矩阵转换为取均值的评分矩阵
    val removeAvgRatingUserMoviesRatingRDD = userMoviesRatingRDD
      .map(record => (record.userID, record))
      .join(userAvgRatingRDD)
      .map {
        case (_, (record, avgRating)) => {
          record.rating -= avgRating
          (record.movieID, record)
        }
      }
    // 计算出每个电影的评分人数
    val numberOfRatersPerMoviesRDD = userMoviesRatingRDD
      .map(record => (record.movieID, 1))
      .reduceByKey(_ + _)

    // 关联获取每个电影的评分人数
    val userMovieRatingNumberOfRatesRDD = removeAvgRatingUserMoviesRatingRDD
      .join(numberOfRatersPerMoviesRDD)
      .map {
        case (_, (record, raters)) => {
          (record.userID, (record, raters))
        }
      }

    // 计算出每个用户所用评分下, 电影的数据
    val groupedByUserIDRDD = userMovieRatingNumberOfRatesRDD.groupByKey()


    // 计算电影成对的平均情况
    val moviePairsRDD =groupedByUserIDRDD
      .flatMap{
        case (userID, iter) => {
          // 从iter中获取电影的成对信息
          // 将数据进行一个排序操作(按照电影id进行数据排列)
          val sorted = iter
            .toList
            .sortBy(_._1.movieID)
          // 双层循环获取计算结果并返回
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


    /**
      * 计算余弦相似度
      *
      */
    import spark.implicits._
    // 计算电影的整体的一个评分（也就是物品的相似度矩阵）
    val movieSimilarityRDD = moviePairsRDD
      /* 按照(movieID1, movieID2)键值对进行聚合操作*/
      .groupByKey()
      .mapValues(iter => {
        // 计算当前电影组的相似度
        // iter是一个迭代器，内部的数据类型是：(Double, Int, Double, Int, Double, Double, Double)
        // 对于某一个用户来讲， (movie1的用户评分, movie1的总评分人数, movie2的用户评分, movie2的总评分人数, movie1的评分*movie2的评分，movie1的评分^2，movie2的评分^2）
        // 合并数据
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
        // 开始正式计算相似度
        // 余弦改进公式的计算
        val size = iter.size  // each pair size
        val numofRaters1Int = numOfRaters1.max
        val numofRaters2Int = numOfRaters2.max
        val dotProduct = ratingProduct.sum
        val rating1NormSq = rating1Squared.sum
        val rating2NormSq = rating2Squared.sum
//        val adjuestedCosineCorrelation = dotProduct / (math.sqrt(rating1NormSq) * math.sqrt(rating2NormSq)) * size / (numofRaters1Int * math.log10(numofRaters2Int +10))
        // 必须要使用修正后的余弦相似度来计算
        val adjuestedCosineCorrelation = improvedCosineSimilarity(dotProduct, rating1NormSq, rating2NormSq, size, numofRaters1Int, numofRaters2Int) // 计算效果同上

        // c. 结果输出
        adjuestedCosineCorrelation
      })
      .map {
        case ((movieID1, movieID2), similarity) => {
          (movieID1, movieID2, similarity)
        }
      }


    val movieSimilarityDF = movieSimilarityRDD.toDF("movieID1", "movieID2", "similarity")

    /**
      * movieID => vid
      *
      * tb_vid_index "video_id", "movieID"
      */

    movieSimilarityDF.createOrReplaceTempView("movie_similar_table")

    val movieSimilarityWithVidDF = spark.sql(
      """
        |select b.video_id as ori_item, c.video_id as rec_item, a.similarity as sim
        |from movie_similar_table a
        |join tb_vid_index b
        |on a.movieID1 = b.movieID
        |join tb_vid_index c
        |on a.movieID2 = c.movieID
        |
        |where a.similarity is not null and
        |a.similarity <> 'NaN'
      """.stripMargin)
      .filter("ori_item <> rec_item")
      .withColumn("partition_day", current_date())
      .selectExpr("cast(ori_item as String) ori_item",
        "cast(rec_item as String) rec_item",
        "cast(sim as Double) sim",
        "cast(partition_day as String) partition_day")


    /**
      * 放入mysql,存入本地作为测试
      *
      */

//    val url = "jdbc:mysql://192.168.1.57:3307/test?useUnicode=true&characterEncoding=UTF-8"
    val url = "jdbc:mysql://localhost:3306/test?useUnicode=true&characterEncoding=UTF-8"

    val pro = new Properties()
    pro.put("user","root")
    pro.put("password","741852")
    pro.put("driver", "com.mysql.jdbc.Driver")
    movieSimilarityWithVidDF.write.mode("Overwrite").jdbc(url,"item_based_sim_result",pro)


    spark.stop()
  }

}
