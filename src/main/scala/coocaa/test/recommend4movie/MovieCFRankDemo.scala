package coocaa.test.recommend4movie

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import scala.util.Try

/**
  * Created by Joe.Kwan on 2019/1/9 15:40.
  */
object MovieCFRankDemo {

  def main(args: Array[String]): Unit = {


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
    spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")


    val userMoviesRatingRDD = spark
      .sparkContext.textFile("F:\\projectJ\\data\\ml_itemSimSparkData\\ratings.csv")
      .map(line=> {
        val arr = line.split(",")
        Try(UserMovieRating(arr(0).trim.toInt, arr(1).trim.toInt, arr(2).trim.toDouble))
      })
      .filter(_.isSuccess).map(_.get)


    val moviesRDD = spark.sparkContext.textFile("F:\\projectJ\\data\\ml_itemSimSparkData\\movies.csv")
        .map(line => {
          val arr = line.split(",")
          Try((arr(0).trim.toInt, arr(1).trim))
        })
        .filter(_.isSuccess).map(_.get)


    moviesRDD.take(10).foreach(println)

    val index2UserIDRDD = userMoviesRatingRDD
      .map(record => record.userID)
      .distinct()
      .zipWithIndex()
      .map(_.swap)

    val userNumber = index2UserIDRDD.count()


    // 3.2 获取所有的电影id形成的RDD==> 这里有一个笛卡尔积操作
    val index2MovieIDRDD = moviesRDD
      .flatMap {
        case (movieID, movieName) => {
          (0L until userNumber).map(index => (index, movieID))
        }
      }

    val moviesNumber = moviesRDD.count()
    val resultNumber = index2MovieIDRDD.count()

    println(userNumber, moviesNumber, resultNumber)

    /**
      * 读取相似度数据, 测试使用mysql中的数据
      */
//    val url = "jdbc:mysql://localhost:3306/test?useUnicode=true&characterEncoding=UTF-8"
//
//    val pro = new Properties()
//    pro.put("user","root")
//    pro.put("password","741852")
//    pro.put("driver", "com.mysql.jdbc.Driver")
//
//    val movieSimilarityDF = spark.read.jdbc(url,"item_based_sim_result",pro)



//    val index2UserIDRDD = userMoviesRatingRDD
//      .map(record => record.userID)
//      .distinct()
//      .zipWithIndex()
//      .map(_.swap)
//        index2UserIDRDD.cache()
//        val userNumber = index2UserIDRDD.count()
//        // 3.2 获取所有的电影id形成的RDD
//        val index2MovieIDRDD = moviesRDD
//          .flatMap {
//            case (movieID, movieName) => {
//              (0L until userNumber).map(index => (index, movieID))
//            }
//          }





    spark.stop()


  }

}
