package ml

import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.linalg.{Vectors, SparseVector => MLSV}
import org.apache.spark.sql.SparkSession
import util.{Breeze2SparkConverter, Spark2BreezeConverter}
//import org.apache.spark.mllib.linalg.{Vector, Vectors}

/**
 * Created by Joe.Kwan on 2020/5/11
 */
object SparkTestReadEmbeddingParquet {


//  type Summarizer = MultivariateOnlineSummarizer
//
//  case class VectorSumarizer(f: String) extends Aggregator[Row, Summarizer, Vector]
//    with Serializable {
//    def zero = new Summarizer
//    def reduce(acc: Summarizer, x: Row) = acc.add(x.getAs[Vector](f))
//    def merge(acc1: Summarizer, acc2: Summarizer) = acc1.merge(acc2)
//
//    // This can be easily generalized to support additional statistics
//    def finish(acc: Summarizer) = acc.mean
//
//    def bufferEncoder: Encoder[Summarizer] = Encoders.kryo[Summarizer]
//    def outputEncoder: Encoder[Vector] = ExpressionEncoder()
//  }


  def main(args: Array[String]): Unit = {

    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
    val spark: SparkSession = SparkSession
      .builder()
      .appName(s"${this.getClass.getSimpleName} guanyue".filter(!_.equals('$')))
      .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .enableHiveSupport()
      .getOrCreate()
    spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")
    // 设置orc解析模式 如果分区下没有文件也能在sql也能查询不会抛错
    spark.sqlContext.setConf("spark.sql.hive.convertMetastoreOrc", "false")

    import spark.implicits._

    val ori = spark.read.parquet("/user/guanyue/recommendation/feature/vs_lfm_embedding/vs_lfm_vid_embedding.parquet")
      .select("vid", "vid_embedding")
      .limit(20)

    ori.show(10, false)
    println("来看一下原始的ori")
    println("ori's schema: ", ori.schema)
    //StructType(StructField(vid,StringType,true), StructField(vid_embedding,ArrayType(FloatType,true),true), StructField(__index_level_0__,LongType,true))
    /**
     * +------------+--------------------+-----------------+
     * |         vid|       vid_embedding|__index_level_0__|
     * +------------+--------------------+-----------------+1
     * | r9m9du40000|[0.13052411, 0.13...|                0|
     * |65dmnvok0400|[-0.23718876, -0....|                1|
     * |4mpm70jk0000|[0.22212696, 0.15...|                2|
     * |8kvf9dj80400|[0.2635309, 0.277...|                3|
     * |2fcgp1tk0000|[0.36572444, 0.43...|               17|
     * | r9m9f740000|[0.39764035, 0.40...|               18|
     * |4tj94qd80000|[0.45130146, 0.24...|               19|
     * +------------+--------------------+-----------------+
     */


    val converter = ori.rdd.map {row =>
      val vid = row.getAs[String]("vid")
      val embedding_ori = row.getAs[scala.collection.mutable.WrappedArray[Double]]("vid_embedding").toArray
      (vid, Vectors.dense(embedding_ori).toDense)
    }.toDF("test1", "test2")


    /**
     *
     * +------------+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
     * |test1       |test2                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      |
     * +------------+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
     * |r9m9du40000 |[0.20152547955513,0.37677618861198425,-0.2372724562883377,0.20716461539268494,-0.15078093111515045,0.20958244800567627,-0.2912091016769409,-0.14421065151691437,0.27060309052467346,-0.18728990852832794,0.2174512892961502,0.31510093808174133,0.22089438140392303,-0.21538756787776947,0.17921240627765656,0.38884788751602173,0.3097633421421051,0.12399627268314362,0.10339532792568207,0.24648194015026093,0.1620323657989502,0.12937556207180023,0.3238508999347687,0.19467905163764954,0.2644500732421875,-0.10170018672943115,0.18287354707717896,-0.29854896664619446,0.3641338348388672,-0.16723841428756714,0.3886577785015106,0.18069280683994293]                             |
     * |r9m9f740000 |[0.3457257151603699,-0.19110549986362457,-0.5645160675048828,0.31649672985076904,-0.2628611922264099,0.6289493441581726,-0.4375121593475342,-0.41930463910102844,0.6561195254325867,-0.5596597194671631,0.4148692488670349,0.4604009687900543,0.39439475536346436,-0.3738041818141937,0.3772228956222534,0.626663088798523,-0.7277946472167969,0.434508740901947,0.5476738214492798,0.3875581920146942,0.5110372304916382,0.4386266767978668,0.5620798468589783,-0.48369696736335754,0.3057454526424408,-0.09963466227054596,0.6001965403556824,-0.42169466614723206,-0.5589904189109802,-0.43771329522132874,-0.7494295239448547,0.4416198134422302]                                      |
     * |r98dthg0000 |[0.34432512521743774,-0.45095372200012207,-0.252427875995636,0.337802916765213,-0.33714428544044495,0.20500075817108154,-0.3629038631916046,-0.3576369881629944,0.36351701617240906,-0.38619229197502136,0.333792507648468,0.1860019713640213,0.3950556516647339,-0.38828879594802856,0.39306870102882385,-0.5193586945533752,-0.9533405900001526,0.2859823703765869,0.6310744285583496,0.38039666414260864,0.41157278418540955,0.36959630250930786,0.3919837176799774,-0.39753618836402893,0.20761635899543762,-0.36803796887397766,0.4110265076160431,-0.38088953495025635,-0.36056748032569885,-0.31490039825439453,-0.5171059370040894,0.40783002972602844]                            |
     * |rbkoepo0000 |[0.016808893531560898,0.11828872561454773,0.026167338714003563,-0.036233678460121155,0.07538772374391556,-0.03513268753886223,0.07565632462501526,-0.04582764208316803,-0.13649491965770721,0.14222104847431183,-0.0186180267482996,0.0917457863688469,-0.06354083120822906,0.02545803226530552,-0.040437083691358566,0.06135844439268112,-0.007846405729651451,-0.09299038350582123,0.0036305435933172703,-0.04360280558466911,-0.03812519460916519,-0.05356130003929138,-0.049047064036130905,-0.17325641214847565,-0.09054215252399445,-0.019637269899249077,-0.017378486692905426,0.03135613352060318,0.14635595679283142,0.1379578560590744,-0.02431170456111431,-0.08179467916488647]|
     * |r9ie6s00000 |[-0.1614404320716858,-0.35885903239250183,0.09117843210697174,-0.054258447140455246,0.03131944686174393,0.750593900680542,0.10673265159130096,-0.40902039408683777,0.503096878528595,-0.21741566061973572,-0.08586909621953964,-0.6408021450042725,0.5553549528121948,-0.03310682624578476,0.40330085158348083,0.08678034693002701,0.4415457248687744,0.4556505084037781,0.4785885214805603,0.051009319722652435,-0.12213946133852005,0.11309140175580978,0.012856503948569298,0.02803865447640419,-0.005210315342992544,-0.2207893282175064,0.40130776166915894,0.10535556077957153,-0.24415995180606842,-0.43260475993156433,0.01357048749923706,-0.39770275354385376]                   |
     * |r93seas0000 |[0.06817574799060822,0.4634717106819153,-0.32987743616104126,0.2969367802143097,-0.1168566346168518,0.115833580493927,-0.0955846756696701,-0.2853924036026001,0.1971653699874878,-0.23975369334220886,0.19230811297893524,0.3596815764904022,0.377322793006897,-0.15848319232463837,0.3564022481441498,-0.952239990234375,0.2371031492948532,0.14955702424049377,0.28517740964889526,0.06304170936346054,0.21817567944526672,0.10781081020832062,0.45535677671432495,0.5596292614936829,0.18824826180934906,-0.3363250494003296,0.24116714298725128,-0.2817451059818268,0.10869774967432022,-0.13353070616722107,0.43933239579200745,0.18949180841445923]                                  |
     * |2fcgp1tk0000|[0.0955788642168045,0.5092877745628357,-0.2881759703159332,0.4185134470462799,-0.3909880816936493,0.062180303037166595,-0.5585214495658875,0.5493890643119812,-0.42553383111953735,0.6726669073104858,0.38181501626968384,0.10077745467424393,-0.5057432651519775,-0.1154920682311058,-0.5750751495361328,0.5789732933044434,-0.266723096370697,-0.5448334217071533,0.17895886301994324,-0.2728259265422821,-0.550112247467041,0.6112802028656006,0.2674884796142578,-0.6212736964225769,-0.3502231538295746,-0.059838369488716125,-0.20897293090820312,-0.3897933065891266,-0.5567097067832947,0.5850093364715576,0.4824310839176178,-0.4276008605957031]                                 |
     * |81o87s5s0400|[0.3593793213367462,-0.43855389952659607,-0.380056232213974,0.31834039092063904,-0.47165465354919434,0.17593976855278015,-0.38239946961402893,-0.3232562243938446,0.3406789004802704,-0.2993483543395996,0.38290658593177795,0.5274385809898376,0.2617579400539398,-0.2620946168899536,0.14266957342624664,0.20079877972602844,-0.835908055305481,0.35531049966812134,0.08939878642559052,0.41780638694763184,0.3482298254966736,0.3647039830684662,0.36864879727363586,-0.7152610421180725,0.2994377613067627,-0.32221466302871704,0.4065006673336029,-0.3670860528945923,-0.7062957882881165,-0.4240496754646301,-0.4843878746032715,0.24717755615711212]                                |
     * |4ma35mg40000|[0.433321088552475,-0.019028980284929276,-0.18795135617256165,0.14776116609573364,-0.25312384963035583,0.17726916074752808,-0.2952822148799896,-0.4431089162826538,0.7888146638870239,0.6483519673347473,0.35134634375572205,0.002878757193684578,0.1802702397108078,-0.4408825933933258,0.06507790088653564,-0.33163413405418396,-0.36476725339889526,0.5644733905792236,0.6391785144805908,0.268961638212204,0.05358605459332466,0.41489022970199585,0.1014150083065033,-0.5197162628173828,0.42289599776268005,-0.21558405458927155,-0.41692376136779785,-0.14143307507038116,-0.5675948858261108,-0.228202223777771,-0.5220755934715271,0.44072285294532776]                           |
     * |53ef3a0s0000|[0.35343697667121887,-0.4387681186199188,0.02842138335108757,0.38322117924690247,-0.4176304042339325,0.5290958881378174,-0.3558138906955719,-0.46782591938972473,0.6075601577758789,-0.24201682209968567,0.3930169641971588,-0.6974626183509827,0.5089205503463745,-0.39438819885253906,0.45430153608322144,-0.539678156375885,-0.004522503819316626,0.4873723089694977,0.14578573405742645,0.42057496309280396,0.45109957456588745,0.4176347851753235,0.37831541895866394,-0.5988507270812988,0.44136252999305725,-0.42787519097328186,0.38805997371673584,-0.3767091631889343,0.7060639262199402,-0.4577144682407379,-0.42442214488983154,0.47404035925865173]                           |
     * +------------+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
     * only showing top 10 rows
     * 来看一下转换后的converter
     * (converter's schema: ,StructType(StructField(test1,StringType,true), StructField(test2,org.apache.spark.ml.linalg.VectorUDT@3bfc3ba7,true)))
     */
    converter.show(10,false)
    println("来看一下转换后的converter")

    println("converter's schema: ", converter.schema )

    spark.stop()
  }

}