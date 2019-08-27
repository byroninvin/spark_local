package demo.sparksql.basic.SparkMlLibTest

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.log4j.Logger
import org.apache.log4j.Level


object RddTest {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("rdd_test01").setMaster("local[*]")
    val sc = new SparkContext(conf)
    Logger.getRootLogger.setLevel(Level.WARN)

    //2.1
    val data = Array(1, 2, 3, 4, 5, 6, 7, 8, 9)
    val disData = sc.parallelize(data, 3)


    val distFile1 = sc.textFile("F:\\projectJ\\data\\texttest")
    val distFile2 = sc.textFile("F:\\projectJ\\data\\texttest\\*.txt")
//    distFile2.foreach(x=>println(x))
    // distFile2.count

    // 2.1.2 RDD转换操作
    val rdd1 = sc.parallelize(1 to 9, 3)
    val rdd2 = rdd1.map(x => x*2)
//    rdd2.foreach(x =>println(x))

    val rdd3 = rdd2.filter(x => x >10)
//    rdd3.foreach(x => println(x))

    val rdd4 = rdd3.flatMap(x => x to 20)
    rdd4.foreach(x=>println(x))



  }
}
