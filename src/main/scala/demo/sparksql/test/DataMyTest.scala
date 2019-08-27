package demo.sparksql.test

import coocaa.test.common.DateUtil
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.regexp_replace

/**
  * Created by Joe.Kwan on 2018/12/11 15:35.
  */
object DataMyTest {

  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession
      .builder()
      .appName(s"${this.getClass.getSimpleName} guanyue".filter(!_.equals('$')))
      .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()



//    val date_Str =  DateUtil.getFormatDate(DateUtil.getCurrDate, "yyyy-MM-dd")
//    val date_Str =  DateUtil.getDateBeforeDay
//
//    val n_day_before = DateUtil.getFormatDate(DateUtil.getDateBeforeOrAfter(DateUtil.getDateFromString(date_Str, "yyyy-MM-dd"), -7), "yyyy-MM-dd");
//
//
//
//
//    println(date_Str)
//    println(n_day_before)

//    val date = conf.get("spark.input.date", DateUtil.getDateBeforeDay)
//    val inputDateBeforeOneDay = conf.get("spark.input.date", DateUtil.getDateBeforeDay)


    val date =  DateUtil.getDateBeforeDay
    val day= DateUtil.getFormatDate(DateUtil.getDateBeforeOrAfter(DateUtil.getDateFromString(date, "yyyy-MM-dd"), -1), "yyyy-MM-dd")


    println(date, day)



    import spark.implicits._

    val myDF = spark.sparkContext.parallelize(Seq(
      ("[我]", "关越"),
      ("[ta]", "liub")
    )).toDF("cate", "name")


    myDF.select(regexp_replace($"cate", "\\[|\\]","").alias("category"), $"name")
      .show()



    spark.stop()
  }

}
