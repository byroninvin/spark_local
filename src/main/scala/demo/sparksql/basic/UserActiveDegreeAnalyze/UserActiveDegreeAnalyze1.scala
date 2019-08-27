package demo.sparksql.basic.UserActiveDegreeAnalyze

import demo.sparksql.basic.ConfigTest
import org.apache.spark.sql.DataFrame


/**
  * Created by Joe.Kwan on 2018/8/27. 
  */
object UserActiveDegreeAnalyze1 extends ConfigTest {


  val spark = buildSparkSession("user_active_degree_analyze")

  val startDate = "2016-09-01"
  val endDate = "2016-11-01"

  def main(args: Array[String]): Unit = {

    val userBaseInfo: DataFrame = spark.read.json("F:\\projectJ\\data\\userActive\\user_base_info.json")
    val userActionLog: DataFrame = spark.read.json("F:\\projectJ\\data\\userActive\\user_action_log.json")

    // 第二个功能, 统计指定时间范围内消费金额最多的10个用户
    // 对金额进行处理的函数讲解

    import org.apache.spark.sql.functions._
    import spark.implicits._
    userActionLog
      .filter("actionTime >= '"+ startDate+"' and actionTime <='"+endDate+"' and actionType = 1")
      .join(userBaseInfo, userActionLog("userId")===userBaseInfo("userId"))
      .groupBy(userBaseInfo("userId"), userBaseInfo("username"))
      .agg(round(sum(userActionLog("purchaseMoney")),2).alias("totalPurchaseMoney"))
      .sort($"totalPurchaseMoney".desc)
      .limit(10)
      .show()


    spark.stop()

  }


}
