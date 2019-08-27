package demo.sparksql.basic.GroupFeacher

import java.net.URL

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object GroupFavTeacher1 {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("FavTeacher").setMaster("local[4]")
    val sc = new SparkContext(conf)

    //指定以后从哪里读取数据
    val lines: RDD[String] = sc.textFile("F:\\projectJ\\data")
    //整理数据
    val subjectAndteacher: RDD[((String, String), Int)] = lines.map(line => {
      val index = line.lastIndexOf("/")
      val teacher2 = line.substring(index + 1)
      val httphost = line.substring(0, index)
      val subject = new URL(httphost).getHost.split("[.]")(0)
      ((subject, teacher2), 1)
    })

    //和一组合在一起(这种方式不好,调用了两次map方法
//    val map: RDD[((String, String), Int)] = subjectAndteacher.map((_, 1))

    //聚合, 将学科和老师联合当作key
    val reduced: RDD[((String, String), Int)] = subjectAndteacher.reduceByKey(_+_)

    //分组排序(按学科进行分组)
    val grouped: RDD[(String, Iterable[((String, String), Int)])] = reduced.groupBy(_._1._1)

    //[学科, 该学科对应的数据], 需要指定分组类型
//    val grouped: RDD[(String, Iterable[((String, String), Int)])] = reduced.groupBy((t: ((String, String), Int))=> t._1._1, 4)

    //经过分组之后,一个分区内可能有多个学科的数据, 一个学科就是一个迭代器,
    //将每一个组拿出来进行操作

    //为什么可以调用scala的sortby的方法呢? 因为一个学科的数据已经在一台机器上的一个scala集合里了
    val sorted: RDD[(String, List[((String, String), Int)])] = grouped.mapValues(_.toList.sortBy(_._2).reverse.take(3))

    //为什么不调用RDD的sortBy, 只能全局排序, 而我们希望是在组内排序
    val result: Array[(String, List[((String, String), Int)])] = sorted.collect()

    println(result.toBuffer)

    sc.stop()

  }

}
