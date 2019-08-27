package demo.sparksql.basic.UserActiveDegreeAnalyze

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Created by Joe.Kwan on 2018/8/28. 
  */
object UserActiveDegreeAnalyze2 {

  case class UserActionLog(logId: Long, userId: Long, actionTime: String, actionType: Long, purchaseMoney: Double)
  // VO中间过程
  case class UserActionLogVO(logId: Long, userId: Long, actionValue: Long)


  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder().master("local[*]").appName("user_active3").getOrCreate()

    val userBaseInfo: DataFrame = spark.read.json("F:\\projectJ\\data\\userActive\\user_base_info.json")
    val userActionLog: DataFrame = spark.read.json("F:\\projectJ\\data\\userActive\\user_action_log.json")

    /**
      * 统计最近一个周期相比上一个周期访问次数增长最多的10个用个
      */

    // 比如统计周期是一个月
    // 我们有1个用户,张三,张三在9月份这个周期总共访问了100次, 张三在10月份这个周期总共访问200次
    // 张三这个用户在最近一个周期相比上一个周期,访问次数增长了100次
    // 每个用户都可以计算这么一个值
    // 获取在最近两个周期内,访问次数增长最多的10个用户
    // 周期, 是可以由用户在WEB界面上填写, java web系统会写入mysql,我们可以去获取本次执行的周期
    // 假定1个月, 2016-10-01, 2016-10-31, 上一个周期2016-09-01, 2016-09-30

    import org.apache.spark.sql.functions._
    import spark.implicits._

    // 计算周期内的数据
    val userActionLogInFirstPeriod = userActionLog.as[UserActionLog]
        .filter("actionTime >= '2016-10-01' and actionTime <= '2016-10-31' and actionType = 0")
        .map(userAcitonLogEntry => UserActionLogVO(userAcitonLogEntry.logId, userAcitonLogEntry.userId, 1))


    // 上一个周期的数据我们给他一个-1
    val userActionLogInSecondPeriod = userActionLog.as[UserActionLog]
      .filter("actionTime >= '2016-09-01' and actionTime <= '2016-09-30' and actionType = 0")
      .map(userAcitonLogEntry => UserActionLogVO(userAcitonLogEntry.logId, userAcitonLogEntry.userId, -1))


    val userActionLogDS = userActionLogInFirstPeriod.union(userActionLogInSecondPeriod)

    userActionLogDS
        .join(userBaseInfo, userActionLogDS("userId")===userBaseInfo("userId"))
        .groupBy(userBaseInfo("userId"), userBaseInfo("userName"))
        .agg(sum(userActionLogDS("actionValue")).alias("actionIncrease"))
        .sort($"actionIncrease".desc)
        .limit(10)
        .show()

    
    spark.stop()

  }
}
