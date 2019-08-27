package demo.sparksql.basic.WordCount

import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.{SparkConf, SparkContext}


object RunWordCount {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.OFF)

    //不打印出日志
    System.setProperty("spark.ui.showConsoleProgress", "false")

    println("开始运行RunWordCount")
    val sc = new SparkContext(new SparkConf().setAppName("wordCount").setMaster("local[*]"))
    println("开始读取文本文件")
    val textFile = sc.textFile("F:\\projectJ\\data\\teacher.log")
    println("开始创建RDD")
    val countsRDD = textFile.flatMap(_.split(" "))
      .map((_, 1))
      .reduceByKey(_ + _)
    println("开始保存到文本文件")
    try{
      countsRDD.saveAsTextFile("F:\\projectJ\\testfolder")
      println("已经保存成功")

    } catch {
      case e: Exception => println("输出目录已经存在, 请先删除原有目录");
    }

    sc.stop()
  }

}
