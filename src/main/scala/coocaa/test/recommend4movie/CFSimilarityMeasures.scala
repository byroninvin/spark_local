package coocaa.test.recommend4movie

/**
  * Created by Joe.Kwan on 2019/1/8 15:14.
  *
  * 协同过滤所使用的相似度公式
  * 主要使用的为修正后的余弦相似度和共现相似度
  *
  */

object CFSimilarityMeasures {

  /**
    * The Co-occurrence similarity between two vectors A, B is
    * |N(i) ∩ N(j)| / sqrt(|N(i)||N(j)|)
    * 两个向量的共现相似度, 与Jaccard是有区别的
    */
  def cooccurrence(numOfRatersForAAndB: Long, numOfRatersForA: Long, numOfRatersForB: Long): Double = {
    numOfRatersForAAndB / math.sqrt(numOfRatersForA * numOfRatersForB)
  }

  /**
    * The correlation between two vectors A, B is
    * cov(A, B) / (stdDev(A) * stdDev(B))
    *
    * This is equivalent to
    * [n * dotProduct(A, B) - sum(A) * sum(B)] /
    * sqrt{ [n * norm(A)^2 - sum(A)^2] [n * norm(B)^2 - sum(B)^2] }
    * 两个向量的相关度
    */
  def correlation(size: Double, dotProduct: Double, ratingSum: Double,
                  rating2Sum: Double, ratingNormSq: Double, rating2NormSq: Double): Double = {

    val numerator = size * dotProduct - ratingSum * rating2Sum
    val denominator = scala.math.sqrt(size * ratingNormSq - ratingSum * ratingSum) *
      scala.math.sqrt(size * rating2NormSq - rating2Sum * rating2Sum)

    numerator / denominator
  }

  /**
    * Regularize correlation by adding virtual pseudocounts over a prior:
    * RegularizedCorrelation = w * ActualCorrelation + (1 - w) * PriorCorrelation
    * where w = # actualPairs / (# actualPairs + # virtualPairs).
    * 正则后的相关度
    */
  def regularizedCorrelation(size: Double, dotProduct: Double, ratingSum: Double,
                             rating2Sum: Double, ratingNormSq: Double, rating2NormSq: Double,
                             virtualCount: Double, priorCorrelation: Double): Double = {

    val unregularizedCorrelation = correlation(size, dotProduct, ratingSum, rating2Sum, ratingNormSq, rating2NormSq)
    val w = size / (size + virtualCount)

    w * unregularizedCorrelation + (1 - w) * priorCorrelation
  }

  /**
    * The cosine similarity between two vectors A, B is
    * dotProduct(A, B) / (norm(A) * norm(B))
    * 余弦相似度
    */
  def cosineSimilarity(dotProduct: Double, ratingNorm: Double, rating2Norm: Double): Double = {
    dotProduct / (ratingNorm * rating2Norm)
  }

  /**
    * The improved cosine similarity between two vectors A, B is
    * dotProduct(A, B) * num(A ∩ B) / (norm(A) * norm(B) * num(A) * log10(10 + num(B)))
    * 修正后的余弦相似度
    */
  def improvedCosineSimilarity(dotProduct: Double, ratingNorm: Double, rating2Norm: Double,
                               numAjoinB: Long, numA: Long, numB: Long): Double = {
    dotProduct * numAjoinB / (ratingNorm * rating2Norm * numA * math.log10(10 + numB))
  }

  /**
    * The Jaccard Similarity between two sets A, B is
    * |Intersection(A, B)| / |Union(A, B)|
    * Jaccard相似度
    */
  def jaccardSimilarity(usersInCommon: Double, totalUsers1: Double, totalUsers2: Double): Double = {
    val union = totalUsers1 + totalUsers2 - usersInCommon
    usersInCommon / union
  }

}
