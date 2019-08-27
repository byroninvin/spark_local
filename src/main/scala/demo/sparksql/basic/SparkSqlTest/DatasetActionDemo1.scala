package demo.sparksql.basic.SparkSqlTest

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
  * Created by Joe.Kwan on 2018/8/29. 
  */
object DatasetActionDemo1 {

  case class Employee(name: String, age: Long, depId: Long, gender: String, salary: Double)
  case class Department(id: Long, name: String)

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().appName("dataset_dataframe").master("local[*]").getOrCreate()

    // loading data
    val department: DataFrame = spark.read.json("F:\\projectJ\\data\\employeeAndDepartment\\department.json")
    val emloyee: DataFrame = spark.read.json("F:\\projectJ\\data\\employeeAndDepartment\\employee.json")

    // 建议转换成强类型的dataset类型
    import spark.implicits._
    val employeeDS = emloyee.as[Employee]
    val departmentDS = department.as[Department]


    // joinWith 两个dataset进行join的算子
    employeeDS.joinWith(departmentDS, employeeDS("depId")===departmentDS("id")).show()


    // sort操作
    employeeDS.sort($"salary".desc).show()

    // randomSplit 分成几个数据集
    // sample 抽样
    val employeeDSArr: Array[Dataset[Employee]] = employeeDS.randomSplit(Array(3, 10, 20))
    employeeDSArr.foreach(ds => ds.show())

    employeeDS.sample(false, 0.3).show()


    spark.stop()
  }

}
