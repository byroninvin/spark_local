package demo.sparksql.basic

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

/**
  * Created by Joe.Kwan on 2018/8/24.
  */
trait ConfigLocalVM {

  def buildSparkSession(appName: String): SparkSession ={

    val session: SparkSession = SparkSession.builder()
      .appName(appName)
      .getOrCreate()
    session

  }

  def SetLogger = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("com").setLevel(Level.OFF)
    System.setProperty("spark.ui.showConsoleProgress","false")
    Logger.getRootLogger().setLevel(Level.OFF)
  }

}
