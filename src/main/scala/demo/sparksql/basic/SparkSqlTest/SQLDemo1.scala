package demo.sparksql.basic.SparkSqlTest

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Joe.Kwan on 2018/8/21. 
  */
object SQLDemo1 {

  def main(args: Array[String]): Unit = {


    // 提交的这个程序可以连接到Spark集群中

    val conf = new SparkConf().setAppName("SQLDemo1").setMaster("local[*]")

    // 创建SparkSQL的连接,相当于程序执行的入口

    val sc = new SparkContext(conf)
    // SparkContext不能创建特殊的RDD(DataFrame)
    // 将SparkContext包装增减
    val sqlContext = new SQLContext(sc)
    // 创建特殊的RDD(DataFrame),就是有schema信息的RDD

    // 先有一个普通的RDD,然后再关联上schema信息,进而转换成DataFrame

    val lines = sc.textFile("hdfs://192.168.9.11:9000/person")
    // 将数据进行整理
    val boyRDD: RDD[Boy] = lines.map(line => {
      val fields = line.split(",")
      val id = fields(0).toLong
      val name = fields(1)
      val age = fields(2).toInt
      val fv = fields(3).toDouble
      Boy(id, name, age, fv)
    })

    // 该RDD转的Boy类型的数据,有了schema信息,但是还是一个RDD
    // 将RDD转换成DataFrame

    // 导入隐式转换
    import sqlContext.implicits._
    val bdf: DataFrame = boyRDD.toDF

    // 变成DF后就可以使用两种API进行编程了

    // 1.使用Sql方式,多执行一步
    // 把DataFrame先注册临时表
    bdf.registerTempTable("t_boy")
    // 书写SQL(SQL方法其实Transformation)
    val result: DataFrame = sqlContext.sql("SELECT * FROM t_boy ORDER BY fv DESC, age ASC")

    // 查看结果(触发Action)
    result.show()

    // 释放资源
    sc.stop()

  }

}

case class Boy(id: Long, name: String, age: Int, fv: Double)