package mab

import breeze.stats.distributions.Beta

import scala.collection.mutable.ListBuffer

/**
 * Created by Joe.Kwan on 2020/2/20
 */
object ThompsonSampling {

  def main(args: Array[String]): Unit = {

    val categoryList = List("a", "b", "c", "d", "e", "f")
    val categoryListShowChick = List(1, 5, 6, 4, 1, 10)
    val categoryListShowNoChick = List(10, 20, 12, 50, 3, 22)

    val result = new ListBuffer[Double]

    for (i <- categoryList.indices) {

      val probBeta = new Beta(categoryListShowChick(i).toDouble, categoryListShowNoChick(i).toDouble)

      result += probBeta.draw()
    }
    println(result)


    val reuslt2 = new ListBuffer[Double]

    for (i <- categoryList.indices) {

      val probBetaClass = new BetaDistribution

      val probBeta = probBetaClass.BetaDist(categoryListShowChick(i).toDouble, categoryListShowNoChick(i).toDouble)

      reuslt2 += probBeta

    }

    println(reuslt2)


  }

}
