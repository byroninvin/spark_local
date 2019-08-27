package demo.sparksql.basic.SparkSqlTest

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
  * Created by Joe.Kwan on 2018/8/29. 
  */
object DatasetTypedOpt {

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

    // 原始的打印一下它的分区数量
    println(employeeDS.rdd.partitions.size)

    // coalesce和repartition操作
    // 都是用来重新第一分区的
    // 区别在于: coalesce,智能用户减少分区数据,而且可以选择不发生shuffle
    // repartition,可以增加分区,也可以减少分区,必须会发生shuffle,相当于是进行了一次重分区操作

    val employeeDSRepartitioned: Dataset[Employee] = employeeDS.repartition(7)

    // 看一下它的分区情况
    println(employeeDSRepartitioned.rdd.partitions.size)
    // coalesce
    val employeeDSCoalesced: Dataset[Employee] = employeeDSRepartitioned.coalesce(3)
    println(employeeDSCoalesced.rdd.partitions.size)

    employeeDSCoalesced.show()
    // employeeDSCoalesced.write.json("F:\\projectJ\\data\\employeeAndDepartment\\employeeWithAgeGreaterThen30DF")


    // distinct和dropDuplicates
    // distinct是根据每一条数据,进行完整内容的比对和去重
    // dropDuplicates是可以根据指定的字段来去重
    // 其实都会进行shuffle的操作,所以比较慢

    val employee1: DataFrame = spark.read.json("F:\\projectJ\\data\\employeeAndDepartment\\employee1.json")

    val employee1DS: Dataset[Employee] = employee1.as[Employee]

    val distinctEmployeeDS: Dataset[Employee] = employee1DS.distinct()

    // 整条文本内容完全一样的话去重
    distinctEmployeeDS.show()
    //
    // val dropDuplicatesDS: Dataset[Employee] = employee1DS.dropDuplicates(Seq("name"))
    val dropDuplicatesDS: Dataset[Employee] = employee1DS.dropDuplicates("name")
    dropDuplicatesDS.show()

    // except 获取在当前dataset中有,但是在另外一个dataset中没有的元素
    // filter 根据我们自己的逻辑,如果返回true,那么就保留该元素,否则就过滤掉改元素
    // intersect 获取两个数据集的交集

    val employee2DS: Dataset[Employee] = spark.read.json("F:\\projectJ\\data\\employeeAndDepartment\\employee2.json").as[Employee]

    employeeDS.except(employee2DS).show()

    employeeDS.filter( employee => employee.age >30).show()

    employeeDS.intersect(employee2DS).show()


    // map 将数据集中的每一条数据都做一个映射,返回一条新数据
    // flatMap 数据集中的每一条数据都可以返回多条数据
    // mapPartititons 一次性对一个partition中的数据进行处理

    employeeDS.map( employee => (employee.name, employee.salary + 1000)).show()

    val departmentDS: Dataset[Department] = department.as[Department]

    departmentDS.flatMap{
      department => Seq(Department(department.id + 1, department.name + "_1"),Department(department.id + 2, department.name + "_2"))
    }.show()

    employeeDS.mapPartitions{employee => {
      val result = scala.collection.mutable.ArrayBuffer[(String, Double)]()
      while (employee.hasNext) {
        var emp = employee.next()
        result += ((emp.name, emp.salary + 1000))
      }
      result.iterator
    }}.show()


    spark.stop()

  }

}
