package demo.sparksql.basic.UDFTest

import java.lang

import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

/**
  * Created by Joe.Kwan on 2018/8/22. 
  */
object UDAFTest {

  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder()
      .appName("UDAFTest")
      .master("local[*]")
      .getOrCreate()

    // UDAF的实例
    val geomean = new GeoMean
    // 还需要注册函数, 非常重要
    spark.udf.register("gm", geomean)



    // 默认下的range的key为id
    val ranged: Dataset[lang.Long] = spark.range(1,11)

    // 将range这个Dataset注册成为视图
    ranged.createTempView("v_range")

    val result1: DataFrame = spark.sql("SELECT gm(id) FROM v_range")

    // 如果不使用注册数据表的方法, 使用DSL风格的方式
    import spark.implicits._
    val result2: DataFrame = ranged.groupBy().agg(geomean($"id").as("geomean"))

    val result3: DataFrame = ranged.agg(geomean($"id").as("geomean"))

    result1.show()
    result2.show()
    result3.show()
    spark.stop()

  }

}

// UDAF自己定义
// ctrl+i 重写方法
class GeoMean extends UserDefinedAggregateFunction{

  // 指定输入数据的类型
  override def inputSchema: StructType = StructType(List(
    StructField("value", DoubleType)
  ))

  // 产生中间结果的数据类型
  override def bufferSchema: StructType = StructType(List(
    // 参与运算数字的个数
    StructField("counts",LongType),
    // 相乘之后返回的积
    StructField("product",DoubleType)
  ))

  // 最终返回的结果类型
  override def dataType: DataType = DoubleType

  // 确保一致性,一般用true
  override def deterministic: Boolean = true

  // 指定初始值
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    // 参与运算数字个数的初始值, 与之前的顺序需要对应
    buffer(0) = 0L
    // 相乘的初始值, 与之前的顺序需要对应
    buffer(1) = 1.0
  }

  // 每有一条数据参与运算就更新一下中间结果(update相当于在每一个分区中的运算)
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    // 参与运算的个数更新(包含中间结果)
    buffer(0) = buffer.getLong(0) + 1L
    // 每有一个数字参与运算就进行相乘(包含中间结果),Row角标为0
    buffer(1) = buffer.getDouble(1) * input.getDouble(0)
  }

  // 全局聚合, 将多个分区的结果进行聚合
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    // 每个分区计算的结果进行相加, buffer2代表下一个分区取出来的结果
    buffer1(0) = buffer1.getLong(0) + buffer2.getLong(0)
    // 每个分区计算的结果进行相乘
    buffer1(1) = buffer1.getDouble(1) * buffer2.getDouble(1)
  }

  // 计算最终结果
  override def evaluate(buffer: Row): Any = {
    math.pow(buffer.getDouble(1), 1.toDouble / buffer.getLong(0))
  }
}