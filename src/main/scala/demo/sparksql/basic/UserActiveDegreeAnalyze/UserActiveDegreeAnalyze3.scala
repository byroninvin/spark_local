package demo.sparksql.basic.UserActiveDegreeAnalyze

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Created by Joe.Kwan on 2018/8/28. 
  */
object UserActiveDegreeAnalyze3 {

  case class UserActionLog(logId: Long, userId: Long, actionTime: String, actionType: Long, purchaseMoney: Double)
  case class UserActionLogVO(logId: Long, userId: Long, actionValue: Long)
  case class UserActionLogWithPurchaseMoney(logId: Long, userId: Long, purchaseMoney: Double)


  def main(args: Array[String]): Unit = {
    /**
      * 统计最近一个周期相比上一个周期购买金额增长最多的10个用户
      * 真实的情况大量都是类似的
      */


    val spark: SparkSession = SparkSession.builder().master("local[*]").appName("user_action").getOrCreate()

    val userActionLog: DataFrame = spark.read.json("F:\\projectJ\\data\\userActive\\user_action_log.json")
    val userBaseInfo: DataFrame = spark.read.json("F:\\projectJ\\data\\userActive\\user_base_info.json")

    import org.apache.spark.sql.functions._
    import spark.implicits._

    val userActionLogWithPurchaseMoneyInFirstPeriod = userActionLog.as[UserActionLog]
      .filter("actionTime >= '2016-10-01' and actionTime <='2016-10-31' and actionType = 1")
      .map(userAcitonLogEntry => UserActionLogWithPurchaseMoney(userAcitonLogEntry.logId, userAcitonLogEntry.userId, userAcitonLogEntry.purchaseMoney))

    val userActionLogWithPurchaseMoneyInSecondPeriod = userActionLog.as[UserActionLog]
      .filter("actionTime >= '2016-09-01' and actionTime <='2016-09-30' and actionType = 1")
      .map(userAcitonLogEntry => UserActionLogWithPurchaseMoney(userAcitonLogEntry.logId, userAcitonLogEntry.userId, -userAcitonLogEntry.purchaseMoney))


    val userActionLogWithPurchaseMoneyDS = userActionLogWithPurchaseMoneyInFirstPeriod.union(userActionLogWithPurchaseMoneyInSecondPeriod)


    userActionLogWithPurchaseMoneyDS
      .join(userBaseInfo, userActionLogWithPurchaseMoneyDS("userId")===userBaseInfo("userId"))
      .groupBy(userBaseInfo("userId"), userBaseInfo("userName"))
      .agg(round(sum(userActionLogWithPurchaseMoneyDS("purchaseMoney")),2).alias("purchaseMoneyIncrease"))
      .sort($"purchaseMoneyIncrease".desc)
      .limit(10)
      .show()

    spark.stop()


  }

}
