package demo.sparksql.basic.WordCount

import java.net.URL

/**
  * Created by Joe.Kwan on 2018/8/28. 
  */
object TestSplit {

  def main(args: Array[String]): Unit = {

    val line = "http://bigdata.edu360.cn/laozhao"

    // 学科,老师
    val splits: Array[String] = line.split("/")

    splits.foreach(x => println(x))

    val subject = splits(2).split("[.]")(0)
    val teacher = splits(3)

    println("-------------")
    println(subject + " " + teacher)


    println("-------------")
    val index = line.lastIndexOf("/")
    val teacher2 = line.substring(index + 1)

    val httphost = line.substring(0, index)

    val hostname = new URL(httphost).getHost.split("[.]")(0)

    println(teacher2, hostname)

  }

}
