//package ml.fm
//
//import org.apache.spark.mllib.linalg.{DenseMatrix, Vector, Vectors}
//import org.apache.spark.mllib.optimization.LBFGS
//import org.apache.spark.mllib.regression.LabeledPoint
//import org.apache.spark.rdd.RDD
//import org.apache.spark.storage.StorageLevel
//
//import scala.util.Random
//
///**
// * Created by Joe.Kwan on 2020/1/29
// */
//object FMWithLBFGS {
//
//
//  def train(input: RDD[LabeledPoint],
//            task: Int,
//            numIterations: Int,
//            numCorrections: Int,
//            dim: (Boolean, Boolean, Int),
//            regParam: (Double, Double, Double),
//            initStd: Double): FMModel = {
//    new FMWithLBFGS(task, numIterations, numCorrections, dim, regParam)
//      .setInitStd(initStd)
//      .run(input)
//  }
//}
//
//
//class FMWithLBFGS(private var task: Int,
//                  private var numIterations: Int,
//                  private var numCorrections: Int,
//                  private var dim: (Boolean, Boolean, Int),
//                  private var regParam: (Double, Double, Double)) extends Serializable {
//
//  private var k0: Boolean = dim._1
//  private var k1: Boolean = dim._2
//  private var k2: Int = dim._3
//
//  private var r0: Double = regParam._1
//  private var r1: Double = regParam._2
//  private var r2: Double = regParam._3
//
//  private var initMean: Double = 0
//  private var initStd: Double = 0.01
//
//  private var numFeatures: Int = -1
//  private var minLabel: Double = Double.MaxValue
//  private var maxLabel: Double = Double.MinValue
//
//
//  def setDim(dim: (Boolean, Boolean, Int)): this.type = {
//    require(dim._3 > 0)
//    this.k0 = dim._1
//    this.k1 = dim._2
//    this.k2 = dim._3
//    this
//  }
//
//
//  /**
//   * @param regIntercept intercept regularization
//   * @param reg1Way one-way interactions regularization
//   * @param reg2Way pairwise interactions regularization
//   */
//  def setRegParam(regIntercept: Double = 0, reg1Way: Double = 0, reg2Way: Double = 0): this.type = {
//    setRegParam((regIntercept, reg1Way, reg2Way))
//  }
//
//
//  /**
//   * @param initStd Standard Deviation used for factorization matrix initialization.
//   */
//  def setInitStd(initStd: Double): this.type = {
//    require(initStd > 0)
//    this.initStd = initStd
//    this
//  }
//
//
//  /**
//   * Set the number of iterations for SGD.
//   */
//  def setNumIterations(numIterations: Int): this.type = {
//    require(numIterations > 0)
//    this.numIterations = numIterations
//    this
//  }
//
//
//  /**
//   * Encode the FMModel to a dense vector, with its first numFeatures * numFactors elements representing the
//   * factorization matrix v, sequential numFeatures elements representing the one-way interactions weights w if k1 is
//   * set to true, and the last element representing the intercept w0 if k0 is set to true.
//   * The factorization matrix v is initialized by Gaussinan(0, initStd).
//   * v : numFeatures * numFactors + w : [numFeatures] + w0 : [1]
//   */
//  private def generateInitWeights(): Vector = {
//    (k0, k1) match {
//      case (true, true) =>
//        Vectors.dense(Array.fill(numFeatures * k2)(Random.nextGaussian() * initStd + initMean) ++
//          Array.fill(numFeatures + 1)(0.0))
//
//      case (true, false) =>
//        Vectors.dense(Array.fill(numFeatures * k2)(Random.nextGaussian() * initStd + initMean) ++
//          Array(0.0))
//
//      case (false, true) =>
//        Vectors.dense(Array.fill(numFeatures * k2)(Random.nextGaussian() * initStd + initMean) ++
//          Array.fill(numFeatures)(0.0))
//
//      case (false, false) =>
//        Vectors.dense(Array.fill(numFeatures * k2)(Random.nextGaussian() * initStd + initMean))
//    }
//  }
//
//
//  /**
//   * Create a FMModel from an encoded vector.
//   */
//  private def createModel(weights: Vector): FMModel = {
//
//    val values = weights.toArray
//
//    val v = new DenseMatrix(k2, numFeatures, values.slice(0, numFeatures * k2))
//
//    val w = if (k1) Some(Vectors.dense(values.slice(numFeatures * k2, numFeatures * k2 + numFeatures))) else None
//
//    val w0 = if (k0) values.last else 0.0
//
//    new FMModel(task, v, w, w0, minLabel, maxLabel)
//  }
//
//  /**
//   * Run the algorithm with the configured parameters on an input RDD
//   * of LabeledPoint entries.
//   */
//  def run(input: RDD[LabeledPoint]): FMModel = {
//
//    if (input.getStorageLevel == StorageLevel.NONE) {
//      println("The input data is not directly cached, which may hurt performance if its"
//        + " parent RDDs are also uncached.")
//    }
//
//    this.numFeatures = input.first().features.size
//    require(numFeatures > 0)
//
//    if (task == 0) {
//      val (minT, maxT) = input.map(_.label).aggregate[(Double, Double)]((Double.MaxValue, Double.MinValue))({
//        case ((min, max), v) =>
//          (Math.min(min, v), Math.max(max, v))
//      }, {
//        case ((min1, max1), (min2, max2)) =>
//          (Math.min(min1, min2), Math.max(max1, max2))
//      })
//
//      this.minLabel = minT
//      this.maxLabel = maxT
//    }
//
//    val gradient = new FMGradient(task, k0, k1, k2, numFeatures, minLabel, maxLabel)
//
//    val updater = new FMUpdater(k0, k1, k2, r0, r1, r2, numFeatures)
//
//    val optimizer = new LBFGS(gradient, updater)
//      .setNumIterations(numIterations)
//
//    val data = task match {
//      case 0 =>
//        input.map(l => (l.label, l.features)).persist()
//      case 1 =>
//        input.map(l => (if (l.label > 0) 1.0 else -1.0, l.features)).persist()
//    }
//
//    val initWeights = generateInitWeights()
//
//    val weights = optimizer.optimize(data, initWeights)
//
//    data.unpersist()
//
//    createModel(weights)
//  }
//
//
//
//}
