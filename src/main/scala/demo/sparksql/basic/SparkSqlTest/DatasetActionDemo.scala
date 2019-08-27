package demo.sparksql.basic.SparkSqlTest

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Created by Joe.Kwan on 2018/8/29. 
  */
object DatasetActionDemo {

  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder().appName("dataset_action").master("local[*]").getOrCreate()

    // loading data
    val department: DataFrame = spark.read.json("F:\\projectJ\\data\\employeeAndDepartment\\department.json")
    val emloyee: DataFrame = spark.read.json("F:\\projectJ\\data\\employeeAndDepartment\\employee.json")

    import spark.implicits._
    // collect 将分布式存储在集群上的分布式数据集(比如说dataset),将所有数据都获取到driver端来
    department.collect().foreach(println)
    // count 对dataset中的记录数,统计个数操作
    println(department.count())
    // first 获取dataset数据中的第一条数据
    println(department.first())
    // foreach 遍历数据集中的每一条数据,对数据进行操作,这个和collect不同,collect是将数据获取到driver端进行操作
    // foreach 是将计算操作推到集群上去,分布式执行
    // foreach(println(_))这种,真正在集群中执行的时候,是没有用的,因为输出的结果是在分布式的集群中,我们是看不到了
    emloyee.foreach(println(_))
    // reuduce 就是对数据集中的所有数据,进行归约的操作,多条变成一条
    // 用reduce来实现数据集个数的统计
    emloyee.map(employee => 1).reduce(_+_)
    // show 默认将dataset数据打印前20条
    emloyee.show()
    // take 从数据集中获得指定条数
    emloyee.take(3).foreach(println)

    spark.stop()

  }

}
