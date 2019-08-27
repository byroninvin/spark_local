package demo.sparksql.basic.GroupFeacher

import java.net.URL

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object GroupFavTeacher2 {

  def main(args: Array[String]): Unit = {

    val subjects = Array("bigdata", "javaee", "php")

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

    //聚合,将学科和老师联合当作key
    val reduced: RDD[((String, String), Int)] = subjectAndteacher.reduceByKey(_+_)

    //RDD的排序, 内存+磁盘

    //其实应该使用cache(标记为cache的RDD以后被反复使用, 才使用cache)
    reduced.cache()

    for (sb <- subjects) {
      //该rdd中对应的数据仅有一个学科的数据,因为过滤了
      val filtered: RDD[((String, String), Int)] = reduced.filter(_._1._1 == sb)

      //rdd的sortBy方法,数据量大也没有关系, take是一个action,会触发任务提交
      val favTeacher: Array[((String, String), Int)] = filtered.sortBy(_._2, false).take(3)

      //打印
      println(favTeacher.toBuffer)
    }

    //前面cache的数据已经计算完成了,后面还有很多其他的指标要计算
    //后面计算的指标也要触发很多次的Action, 最好将数据缓存到内存
    //原来的数据占用着内存,把原来的数据释放掉,才能缓存新的数据
    
//    reduced.unpersist(true)   //等待,释放完成,再执行

    sc.stop()
  }
}
