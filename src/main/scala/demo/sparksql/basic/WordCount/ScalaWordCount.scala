package demo.sparksql.basic.WordCount

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Joe.Kwan on 2018/8/28. 
  */
object ScalaWordCount {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
    // 创建spark执行的入口
    val sc = new SparkContext(conf)
    // 指定以后从哪里读取数据创建RDD (弹性分布式数据集)
    sc.textFile(args(0))

  }

}
