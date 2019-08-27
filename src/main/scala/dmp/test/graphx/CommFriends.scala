package dmp.test.graphx

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Joe.Kwan on 2018/9/4. 
  */
object CommFriends {


  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf()
    sparkConf.setAppName(s"${this.getClass.getSimpleName}")
    sparkConf.setMaster("local[*]")
    //
    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    val sc = new SparkContext(sparkConf)







    sc.stop()
  }
}
