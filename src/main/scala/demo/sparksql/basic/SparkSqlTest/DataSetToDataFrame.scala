package demo.sparksql.basic.SparkSqlTest

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Created by Joe.Kwan on 2018/8/29. 
  */
object DataSetToDataFrame {

  case class Employee(name: String, age: Long, depId: Long, gender: String, salary: Double)

  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder().appName("dataset_dataframe").master("local[*]").getOrCreate()


    // loading data
    val department: DataFrame = spark.read.json("F:\\projectJ\\data\\employeeAndDepartment\\department.json")
    val emloyee: DataFrame = spark.read.json("F:\\projectJ\\data\\employeeAndDepartment\\employee.json")

    // 持久化
    // 如果要对一个dataset重复计算两次的话, 那么建议想对这个dataset进行持久化再进行操作,避免重复计算

    emloyee.cache()
    println(emloyee.count())
    println(emloyee.count())

    // 创建临时视图,直接对数据执行sql语句
    emloyee.createOrReplaceTempView("emplyee")
    spark.sql("select * from emplyee where age > 30").show()

    // 获取sparksql的执行计划
    // dataframe/dataset,比如执行了一个sql语句获取的dataframe,实际上内部包含一个logical plan,逻辑执行计划
    // 实际执行的时候,首先会通过底层的catalyst optimize,生成物理执行计划,比如说做一些优化
    // 比如push filter还会通过whole-stage code generation技术去自动化生成代码,提升执行性能
    spark.sql("select * from emplyee where age >30").explain()

    // printSchema, 自动推断
    emloyee.printSchema()

    // dataframe和dataset之间的互相转化
    // dataframe是untype类型的
    import spark.implicits._
    val employeeDS = emloyee.as[Employee]
    employeeDS.show()
    employeeDS.printSchema()

    // dataset->dataframe
    val employeeDF = employeeDS.toDF()
    employeeDF.show()
    employeeDF.printSchema()


    // 写数据
    // val employeeWithAgeGreaterThen30DF: DataFrame = spark.sql("select * from emplyee where age >30")
    // employeeWithAgeGreaterThen30DF.write.json("F:\\projectJ\\data\\employeeAndDepartment\\employeeWithAgeGreaterThen30DF")





    spark.stop()

  }

}
