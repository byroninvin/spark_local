package demo.sparksql.basic.IOTest

import java.util.Properties

import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

/**
  * Created by Joe.Kwan on 2018/8/22. 
  */
object JdbcTest {

  def main(args: Array[String]): Unit = {


    val spark: SparkSession = SparkSession.builder()
      .appName("UDAFTest")
      .master("local[*]")
      .getOrCreate()


    // load的这个方法会读取真正mysql的数据吗?会不会和数据库建立连接,会需要获取表头信息
    // DataFrame并没有保存真正的信息
    val logs: DataFrame = spark.read.format("jdbc").options(
      Map("url" -> "jdbc:mysql://localhost:3306/test",
        "driver" -> "com.mysql.jdbc.Driver",
        "dbtable" -> "domain_object_discern",
        "user" -> "root",
        "password" -> "741852")
    ).load()

    // 查看Schema信息
    // logs.printSchema()

    // 查看表信息
//     logs.show()

    // 筛选过滤, 这种方法感觉不够方便
//    val filtered: Dataset[Row] = logs.filter(r => {
//      r.getAs[Long](0) <= 1067531
//    })
//    filtered.show()

    // 更加简单的方法lambda表达式,需要导入隐式转换
    import spark.implicits._
    // filter和where方法等同
//    val result: Dataset[Row] = logs.filter($"id" <= 1067531)
    val result: Dataset[Row] = logs.where($"id" <=1067531)

    // 判断条件再,增加列
    val result2: DataFrame = result.select($"id", $"object_type", $"author_id" /10000000 as "test_author_id")


    // 往数据库里面写数据
    val props = new Properties()
    props.put("user", "root")
    props.put("password", "741852")
    props.put("driver", "com.mysql.jdbc.Driver")

    // 如果表已经存在,它及不追加也不覆盖
//    result.write.mode("ignore")
    result.write.mode("Overwrite")
      .jdbc("jdbc:mysql://localhost:3306/test", "logs11",props)


    // 保存到本地
    // write.text并不可以,必须是只能有一列的情况下不能保存
//    result.write.text("F:\\projectJ\\data\\test")
     // 其实Dataset就是只有一列的数据集
//    result.select($"weibo_id").write.text(("F:\\projectJ\\data\\test1"))


    // 保存成为csv
//    result2.write.csv("F:\\projectJ\\data\\test3")


    // 保存成为json
//    result2.write.json("F:\\projectJ\\data\\test2")


    // 保存成为parquet文件,不但被parquet存储了还被压缩了
//    result2.write.parquet("F:\\projectJ\\data\\test2")


    // 需要配置参数才可以写入hdfs中
//    result2.write.parquet("hdfs://192.168.9.11:9000/parquet")
//    result2.write.parquet("F:\\projectJ\\data\\test4")



//    result.show()
//    result2.show()
    spark.stop()
  }

}
