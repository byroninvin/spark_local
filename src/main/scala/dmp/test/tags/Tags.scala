package dmp.test.tags

/**
  * Created by Joe.Kwan on 2018/9/4. 
  */
trait Tags {

  /**
    * 打标签的方法定义
    * @param args
    * @return
    */

  def makeTags(args: Any*): Map[String, Int]

}
