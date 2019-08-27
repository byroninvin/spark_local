package demo.sparksql.basic.SparkSqlTest

import java.net.URL

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
  * Created by Joe.Kwan on 2018/8/22. 
  */
object SQLFavTeacher {

  def main(args: Array[String]): Unit = {


    val spark: SparkSession = SparkSession.builder()
      .appName("SQLFavTeacher")
      .master("local[*]")
      .getOrCreate()


    // 参数一
    val lines: Dataset[String] = spark.read.textFile(args(0))


    // 参数二
    val topN = args(1).toInt


    import spark.implicits._

    // 数据处理
    val df: DataFrame = lines.map(line => {
      val tIndex = line.lastIndexOf("/") + 1
      val teacher = line.substring(tIndex)
      val host = new URL(line).getHost

      // 学科的index
      val sIndex = host.indexOf(".")
      val subject = host.substring(0, sIndex)

      (subject, teacher)

    }).toDF("subject", "teacher")


    // 注册成一张视图
    df.createTempView("v_sub_teacher")

    // 该学科下面的老师的访问次数
    val temp1: DataFrame = spark.sql("SELECT subject, teacher, count(*) counts FROM v_sub_teacher GROUP BY subject, teacher")

    // 求每个学科下最受欢迎的老师TOPN
    temp1.createTempView("v_temp_sub_teacher_counts")

    // 从子查询中查询
    val temp2: DataFrame = spark.sql("SELECT subject, teacher, counts, row_number() over(partition by subject order by counts desc) rk FROM v_temp_sub_teacher_counts")



    // 即想取全局排名,又想取局部排名,sql里面的窗口函数
    // 全局排序rank() over() 和 row_number() over()功能是差不多的
    val temp3: DataFrame = spark.sql(s"SELECT * FROM (SELECT subject, teacher, counts, row_number() over(partition by subject order by counts desc) rk, rank() over(order by counts desc) g_rk FROM v_temp_sub_teacher_counts) WHERE rk <=$topN")

    // 先把入选的老师进行全局排序, 如果有相等的会有并列 dense_rank(), rank() 有细微差别
    val temp4: DataFrame = spark.sql(s"SELECT *, rank() over(order by counts desc) g_rk FROM (SELECT subject, teacher, counts, row_number() over(partition by subject order by counts desc) rk FROM v_temp_sub_teacher_counts) WHERE rk <=$topN")

    temp4.show()

    spark.stop()

  }

}
