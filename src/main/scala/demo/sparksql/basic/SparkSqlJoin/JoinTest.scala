package demo.sparksql.basic.SparkSqlJoin

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
  * Created by Joe.Kwan on 2018/8/22. 
  */
object JoinTest {

  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder()
      .appName("JoinTest")
      .master("local[*]")
      .getOrCreate()

    // 造一些数据, 这里需要加入隐式转换
    import spark.implicits._

    val lines: Dataset[String] = spark.createDataset(
      List("1,laozhao,China", "2,laoduan,usa")
    )

    // 对数据进行整理
    val tpDs: Dataset[(Long, String, String)] = lines.map(line => {
      val fields = line.split(",")
      val id = fields(0).toLong
      val name = fields(1)
      val nationCode = fields(2)
      (id, name, nationCode)
    })
    // 转换成DataFrame
    val df1: DataFrame = tpDs.toDF("id", "name", "nation")


    // 再造一份数据
    val nations: Dataset[String] = spark.createDataset(
      List("China,中国", "usa,美国")
    )

    // 对数据进行整理
    val nationDs: Dataset[(String, String)] = nations.map(line => {
      val fields = line.split(",")
      val ename = fields(0)
      val cname = fields(1)
      (ename, cname)
    })
    // 转换成DataFrame
    val df2: DataFrame = nationDs.toDF("ename", "cname")



    // 关联方式
    // 第一种,创建两个视图
    df1.createTempView("v_users")
    df2.createTempView("v_nations")

    val result1: DataFrame = spark.sql("SELECT * FROM v_users a JOIN v_nations b ON a.nation = b.ename")


    // 关联方式
    // 第二中,使用Dataset方法

    // 如果key是columns是一样的,则可以使用joinWith
    // val df4=df1.joinWith(df2,df1("key1")===df2("key1"))

    // 基于两个公共字段key1和key的等值连接
    // scala> val df10 = df1.join(df2, Seq("key1","key2"))
    // 如果不一样,方法
    val result2: DataFrame = df1.join(df2, df1("nation")===df2("ename"), "inner")

    // scala> val df11 = df1.join(df2, df1("key1")===df2("key1") && df1("key2")>df2("key2"))

    val result3: DataFrame = df1.join(df2, $"nation"===$"ename", "inner")

    result1.show()

    result2.show()

    result3.show()

    spark.stop()

  }

}
