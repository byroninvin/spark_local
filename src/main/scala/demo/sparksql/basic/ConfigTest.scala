package demo.sparksql.basic

import org.apache.spark.sql.SparkSession

/**
  * Created by Joe.Kwan on 2018/8/24.
  */
trait ConfigTest {

  def buildSparkSession(appName: String): SparkSession ={

    val session: SparkSession = SparkSession.builder()
      .appName(appName)
      .master("local[*]")
      .getOrCreate()

    session

  }

}
