package demo.sparksql.basic.UDFTest

/**
  * Created by Joe.Kwan on 2018/8/22. 
  */
object TestPow {

  def main(args: Array[String]): Unit = {

    val i = 1 * 2 * 3

    val r: Double = Math.pow(i, 1.toDouble/3)

    println(r)

  }

}
