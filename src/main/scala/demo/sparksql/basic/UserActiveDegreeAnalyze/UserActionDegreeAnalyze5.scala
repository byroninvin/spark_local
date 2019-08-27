package demo.sparksql.basic.UserActiveDegreeAnalyze

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Created by Joe.Kwan on 2018/8/28. 
  */
object UserActionDegreeAnalyze5 {


  def main(args: Array[String]): Unit = {

    /**
      * 统计指定注册时间范围内头7天购买金额最高的10个用户
      *
      */


    val spark: SparkSession = SparkSession.builder().appName("user_action").master("local[*]").getOrCreate()

    val userActionLog: DataFrame = spark.read.json("F:\\projectJ\\data\\userActive\\user_action_log.json")
    val userBaseInfo: DataFrame = spark.read.json("F:\\projectJ\\data\\userActive\\user_base_info.json")

    import org.apache.spark.sql.functions._
    import spark.implicits._


    userActionLog
      .join(userBaseInfo, userActionLog("userId")===userBaseInfo("userId"))
      .filter(userBaseInfo("registTime")>="2016-10-01"
      && userBaseInfo("registTime")<="2016-10-31"
      && userActionLog("actionTime")>=userBaseInfo("registTime")
      && userActionLog("actionTime")<=date_add(userBaseInfo("registTime"),7)
      && userActionLog("actionType") ===1)

      .groupBy(userBaseInfo("userId"), userBaseInfo("username"))
      .agg(round(sum(userActionLog("purchaseMoney")),2).alias("7daysMoney"))
      .sort($"7daysMoney".desc)
      .limit(10)
      .show()

    spark.stop()



  }

}
