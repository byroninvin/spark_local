package demo.sparksql.basic.SparkSqlTest

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Created by Joe.Kwan on 2018/8/29. 
  */
object DepartmentAVG {


  def main(args: Array[String]): Unit = {
    // create SparkSession
    val spark: SparkSession = SparkSession.builder().master("local[*]").appName("department_avg").getOrCreate()

    // loading data
    val department: DataFrame = spark.read.json("F:\\projectJ\\data\\employeeAndDepartment\\department.json")
    val emloyee: DataFrame = spark.read.json("F:\\projectJ\\data\\employeeAndDepartment\\employee.json")


    import org.apache.spark.sql.functions._

    // department avg age and salary
    emloyee.filter("age>20")
      .join(department, emloyee("depId")===department("id"))
      .groupBy(department("name"), emloyee("gender"))
      .agg(round(avg(emloyee("age")),0).alias("mean_age"), round(avg(emloyee("salary")),2).alias("mean_salary"))
      .show()

    // dataFrame是Dataset[Row]
    // dataFrame的类型是Row,所以是untype类型,弱类型
    // dataset的类型通常是我们定义的case class,所以是typed类型,强类型
    // dataset开发和rdd有很多共同点,transformation,action



    spark.stop()
  }




}
