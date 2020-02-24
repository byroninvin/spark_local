package mab

/**
 * Created by Joe.Kwan on 2020/2/20
 */

/**
 * Generic Arm used in several exploration / exploitation algorithm.
 */
trait Arm {

  val id: Int
  var successes: Int = 1
  var failures: Int = 1

  def mean




}
