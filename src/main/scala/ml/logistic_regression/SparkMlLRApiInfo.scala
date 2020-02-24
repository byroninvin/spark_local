package ml.logistic_regression

import org.apache.spark.SparkConf
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.sql.SparkSession

/**
  * Created by Joe.Kwan on 2019-9-17 16:11.
  */
object SparkMlLRApiInfo {
  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf()
    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    sparkConf.set("spark.rdd.compress", "true")
    sparkConf.set("hive.metastore.warehouse.dir", "hdfs://coocaadata/apps/hive/warehouse")
    sparkConf.set("spark.sql.warehouse.dir", "hdfs://coocaadata/apps/hive/warehouse")
    val spark = SparkSession.builder()
      .config(sparkConf)
      .appName(s"${this.getClass.getSimpleName} guanyue".filter(!_.equals('$')))
      .enableHiveSupport()
      .master("local[*]")
      .getOrCreate()


    /**
      * <1> setMaxIter()：设置最大迭代次数
      *
      * <2> setRegParam()： 设置正则项的参数，控制损失函数与惩罚项的比例，防止整个训练过程过拟合
      *
      * <3> setElasticNetParam()：使用L1范数还是L2范数
      * setElasticNetParam=0.0 为L2正则化；
      * setElasticNetParam=1.0 为L1正则化；
      * setElasticNetParam=(0.0，1.0) 为L1，L2组合
      *
      * <4> setFeaturesCol()：指定特征列的列名，传入Array类型
      *
      * <5>setLabelCol()：指定标签列的列名，传入String类型
      *
      * <6>setPredictionCol()：指定预测列的列名
      *
      * <7>setFitIntercept(value:Boolean)：是否需要偏置，默认为true（即是否需要y=wx+b中的b）
      *
      * <8>setStandardization(value:Boolean)：模型训练时，是否对各特征值进行标准化处理，默认为true
      *
      * <9>setSolver(value:String)：设置用于优化求解器。线性回归支持的有l-bfgs（有限内存拟牛顿法），normal（加权最小二乘法）和auto(自动选择)。
      *
      * <10>setTol(value:Double)：设置迭代的收敛公差。值越小准确性越高但是迭代成本增加。默认值为1E-6。(即损失函数)
      *
      * <11>setWeightCol(value:String)：设置某特征列的权重值，如果不设置或者为空，默认所有实例的权重为1。
      *
      * <12>setAggregationDepth：建议深度大于或等于2，默认为2。如果特征维度较大或者数据的分区量大的时候，可以调大该值。
      *
      * <13>fit：基于训练街训练出模型
      *
      * <14>transform：基于训练出的模型对测试集进行预测
      */

    val lr = new LogisticRegression()





    spark.stop()


  }

}
