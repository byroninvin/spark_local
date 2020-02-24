//package mab
//
//import breeze.stats.distributions.Beta
//
//import scala.util.Random
//
//
///**
// * Created by Joe.Kwan on 2020/2/20
// */
//
//
//trait Arm {
//
//  val id: Int       // 1.
//  var successes: Int = 1        // 2.
//  var failures: Int = 1        // 3.
//
//  def mean: Double = successes.toDouble / (successes + failures)      // 4.
//
//  /**
//   *
//   * @param winner
//   */
//  def apply(winner: Arm): Unit =
//    if (id == winner.id) {
//      winner.successes += 1
//      failures += 1
//    } else {
//      winner.failures += 1
//      successes += 1
//    }
//  override def toString: String = s"id=$id, count${successes + failures}, mean=$mean "
//}
//
//
//trait BetaArm extends Arm {
//  var pdf = new Beta(1, 1)
//
//  override def apply(winner: Arm): Unit = {
//    super.apply(winner)
//    pdf = new Beta(successes+1, failures+1)
//  }
//
//  @inline
//  final def sample: Double = pdf.draw()
//}
//
//
//
//trait CountedArm extends Arm {
//  import Math._
//  var cnt: Int = _
//
//  def select: Unit = cnt += 1
//
//  final def score(numActions: Int): Double = mean + sqrt(2.0 * log(numActions)/cnt)
//}