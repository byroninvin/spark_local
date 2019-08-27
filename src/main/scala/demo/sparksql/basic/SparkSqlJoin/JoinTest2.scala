package demo.sparksql.basic.SparkSqlJoin

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Created by Joe.Kwan on 2018/8/22. 
  */
object JoinTest2 {
  def main(args: Array[String]): Unit = {

    //
    val spark: SparkSession = SparkSession.builder()
      .appName("join_test2")
      .master("local[*]")
      .getOrCreate()


    import spark.implicits._

    val df1: DataFrame = Seq(
      (0, "playing"),
      (1, "with"),
      (2, "join")
    ).toDF("id", "token")

    val df2: DataFrame = Seq(
      (0, "P"),
      (1, "W"),
      (2, "S")
    ).toDF("aid", "atoken")

    // df2.cache().count
    // df1.cache().count


    // broadcast hash join 最大能广播1024*1024*10大的数据(10m大的数据)
    // spark.sql.autoBroadcastJoinThreshold=-1


    // 告诉系统不要广播,及变成了SortMergeJoin
    // spark.conf.set("spark.sql.autoBroadcastJoinThreshold",-1)

    // 100M的广播体谅
    // spark.conf.set("spark.sql.autoBroadcastJoinThreshold", 1024*1024*100)
    // spark.conf.set("spark.sql.preferSortMergeJoin", true)

    //
    // println(spark.conf.get("spark.sql.autoBroadcastJoinThreshold"))

    val query: DataFrame = df1.join(df2, $"id"===$"aid")

    // 查看执行计划
    // 默认选择的是BroadcastHashJoin
    query.explain()

//    query.show()

    spark.stop()
  }

}
