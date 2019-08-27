package demo.sparksql.basic.WordCount

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  *  Created by gy on 2018/08/17
  */

object FavTeacher {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("FavTeacher").setMaster("local[*]")
    val sc = new SparkContext(conf)

    //指定以后从哪里读取数据
    val lines: RDD[String] = sc.textFile("F:\\projectJ\\data\\teacherlog")
    //整理数据
    val teacherAndOne = lines.map(line => {
      val index = line.lastIndexOf("/")
      val teacher2 = line.substring(index + 1)
      //      val httphost = line.substring(0, index)
      //      val subject = new URL(httphost).getHost.split("[.]")(0)
      (teacher2, 1)
    })

    //聚合
    val reduce: RDD[(String, Int)] = teacherAndOne.reduceByKey(_+_)
    val sorted: RDD[(String, Int)] = reduce.sortBy(_._2, false)

    //触发Action执行计算
    val result = sorted.collect()

    //打印
    println(result.toBuffer)
    sc.stop()
  }

}
