package demo.sparksql.basic.SparkSqlTest

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Created by Joe.Kwan on 2018/8/29. 
  */
object AggFuncTest {

  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder().appName("agg_func_gy").master("local[*]").getOrCreate()

    val department: DataFrame = spark.read.json("F:\\projectJ\\data\\employeeAndDepartment\\department.json")
    val employee: DataFrame = spark.read.json("F:\\projectJ\\data\\employeeAndDepartment\\employee1.json")

    import org.apache.spark.sql.functions._

    employee
        .join(department, employee("depId")===department("id"))
        .groupBy(department("name"))
        .agg(avg(employee("salary")), sum(employee("salary")), max(employee("salary")), min(employee("salary")), min(employee("salary")), count(employee("name")),countDistinct(employee("name"))).show()



    // collect_list, collect_set 都用于将同一个分组内的指定字段的值串起来,编程一个数组
    // 常用于行转列
    // 比如说
    // depId = 1, employee = leo
    // depId = 1, employee = jack
    // depId = 1, employee(leo, jack)
    // collect_list是不去重的,collect_set是去重的


    employee
        .groupBy(employee("depId"))
        .agg(collect_list(employee("name")),collect_set(employee("name")))
        .collect()
        .foreach(println(_))


    import org.apache.spark.sql.functions._
    import spark.implicits._
    //常用函数
    employee
        .select(employee("name"),current_date(), rand(), round(employee("salary"),2),concat($"gender", $"age"), concat_ws("||", $"gender", $"age"))
        .show()


    spark.stop()
  }

}
