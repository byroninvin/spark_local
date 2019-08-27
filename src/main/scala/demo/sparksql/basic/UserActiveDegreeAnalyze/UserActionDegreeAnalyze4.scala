package demo.sparksql.basic.UserActiveDegreeAnalyze

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Created by Joe.Kwan on 2018/8/28. 
  */
object UserActionDegreeAnalyze4 {


  def main(args: Array[String]): Unit = {

    /**
      * 统计指定注册时间范围内头7天访问次数最高的10个用户
      * 举例,用户通过web界面指定的注册范围是2016-10-01到2016-10-31
      */

    val spark: SparkSession = SparkSession.builder().appName("user_ation").master("local[*]").getOrCreate()

    val userActionLog: DataFrame = spark.read.json("F:\\projectJ\\data\\userActive\\user_action_log.json")
    val userBaseInfo: DataFrame = spark.read.json("F:\\projectJ\\data\\userActive\\user_base_info.json")


    import org.apache.spark.sql.functions._
    import spark.implicits._

    userActionLog
      .join(userBaseInfo, userActionLog("userId")===userBaseInfo("userId"))
      // 重点在于过滤
      .filter(userBaseInfo("registTime") >= "2016-10-01"
      && userBaseInfo("registTime") <="2016-10-31"
      // drop bug data
      && userActionLog("actionTime") >= userBaseInfo("registTime")
      // date_add
      && userActionLog("actionTime") <= date_add(userBaseInfo("registTime"), 7)
      && userActionLog("actionType") === 0
    ).groupBy(userBaseInfo("userId"), userBaseInfo("username"))
      .agg(count(userActionLog("logId")).alias("actionCount"))
      .sort($"actionCount".desc)
      .limit(10)
      .show()

    spark.stop()

  }

}
