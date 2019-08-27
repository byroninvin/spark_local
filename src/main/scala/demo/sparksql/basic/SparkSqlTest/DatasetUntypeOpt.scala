package demo.sparksql.basic.SparkSqlTest

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Created by Joe.Kwan on 2018/8/29. 
  */
object DatasetUntypeOpt {

  /**
    * untype涵盖了不同sql语法的全部,模拟了整个sql的语法
    * @param args
    */

  def main(args: Array[String]): Unit = {

    // create SparkSession
    val spark: SparkSession = SparkSession.builder().master("local[*]").appName("department_avg").getOrCreate()

    // loading data
    val department: DataFrame = spark.read.json("F:\\projectJ\\data\\employeeAndDepartment\\department.json")
    val emloyee: DataFrame = spark.read.json("F:\\projectJ\\data\\employeeAndDepartment\\employee.json")


    import org.apache.spark.sql.functions._

    import spark.implicits._

    // dataframe的untype操作
    emloyee.where("age>30")
      .join(department, $"depId"===$"id")
      .groupBy(department("name"), emloyee("age"))
      .agg(avg(emloyee("salary")).alias("avg_salary"))
      .sort($"avg_salary".desc)
      .select($"name", $"avg_salary", $"age")
      .show()


    emloyee
        .select($"name", $"depId", $"age").show()


    spark.stop()

  }

}
