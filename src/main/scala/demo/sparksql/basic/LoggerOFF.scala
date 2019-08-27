package demo.sparksql.basic

import org.apache.log4j.{Level, Logger}

/**
  * Created by Joe.Kwan on 2018/8/24.
  */
trait LoggerOFF {

  def SetLogger = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("com").setLevel(Level.OFF)
    System.setProperty("spark.ui.showConsoleProgress","false")
    Logger.getRootLogger().setLevel(Level.OFF)
  }

}
