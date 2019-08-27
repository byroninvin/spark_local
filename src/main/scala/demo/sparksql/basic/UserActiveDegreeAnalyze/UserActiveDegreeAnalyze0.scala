package demo.sparksql.basic.UserActiveDegreeAnalyze

import demo.sparksql.basic.ConfigTest
import org.apache.spark.sql.DataFrame

/**
  * Created by Joe.Kwan on 2018/8/25. 
  */
object UserActiveDegreeAnalyze0 extends ConfigTest{

  val spark = buildSparkSession("user_active_degree_analyze")

  val startDate = "2016-09-01"
  val endDate = "2016-11-01"


  def main(args: Array[String]): Unit = {


    val userBaseInfo: DataFrame = spark.read.json("F:\\projectJ\\data\\userActive\\user_base_info.json")

    val userActionLog: DataFrame = spark.read.json("F:\\projectJ\\data\\userActive\\user_action_log.json")

    // 第一个功能,统计指定时间范围内的访问次数最多的10个用户

//    import spark.implicits._
//
//    userActionLog
//      // 第一步是过滤数据
////        .filter("actionTime >='"+ startDate + "' and actionTime <='" + endDate + "'")
//        .filter($"actionTime" >= startDate)
//
//      // 第二步是关联用户的基本信息数据
//        .join(userBaseInfo, userActionLog("userId")===userBaseInfo("userId"))
//      // 第三步进行分组
//        .groupBy(userBaseInfo("userId"), userBaseInfo("username"))
//      // 第四步进行聚合
//        .agg(count(userActionLog("logId")).alias("actionCount"))
//      // 第五步进行排序
//        .sort($"actionCount".desc)
//        .limit(10).show()

    userActionLog.createOrReplaceTempView("t_action")
    userBaseInfo.createOrReplaceTempView("t_base")


    // 通配符加一个s
     val df2: DataFrame = spark.sql(s"select a.userId, b.username,count(a.logId) actionCount from t_action a join t_base b on a.userId = b.userId where a.actionTime >= '$startDate' and a.actionTime <= '$endDate' group by a.userId, b.username order by actionCount desc limit 25")
    // val df2: DataFrame = spark.sql("select *,count(b.logId) actionCount from t_action a join t_base b on a.userId = b.userId where a.actionTime >= '2016-09-01' and a.actionTime <= '2016-11-01' group by a.userId, a.username")
//    val df2: DataFrame = spark.sql(s"select * from t_action a join t_base b on a.userId = b.userId where a.actionTime >= '$startDate' and a.actionTime <= '$endDate'")
//    val df2: DataFrame = spark.sql(s"select * from t_action where actionTime >= '$startDate'")
    df2.printSchema()
    df2.show()


    spark.stop()
  }




}
