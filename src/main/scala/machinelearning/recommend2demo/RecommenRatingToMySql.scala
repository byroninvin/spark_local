package machinelearning.recommend2demo

import java.util.Properties

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object RecommenRatingToMySql {

  def getMysalProperty: Properties = {
    val prop = new Properties()
    prop.setProperty("user", "root")
    prop.setProperty("password", "741852")
    prop.setProperty("driver", "com.mysql.jdbc.Driver")
    prop
  }

  def main(args: Array[String]): Unit = {

//    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
//    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
    val spark: SparkSession = SparkSession.builder()
      .master("local[*]").appName("recommen_ranking_test")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()

    val df1 = spark.read.option("header", true).csv("F:\\projectDB\\Coocaa\\RecommendationSystem\\Rating\\RatingTestResult.csv").toDF()


//    df1.show()
    df1.write.mode("Overwrite").jdbc("jdbc:mysql://localhost:3306/test", "recommendation_rating_test_result", getMysalProperty)
    spark.stop()

  }

}
