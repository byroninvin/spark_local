package tools.demo.clickhouse

/**
  * Created by Joe.Kwan on 2019-8-23 10:05. 
  */
object clickhouseJDBCtest {
  def main(args: Array[String]): Unit = {
    val sqlDB = "show databases"
    //查询数据库
    val sqlTab = "show tables"
    //查看表
    val sqlCount = "select count(*) count from ontime" //查询ontime数据量

    //clickhouseJDBC.exeSql(sqlDB)

    clickhouseJDBC.exeSql("select count(mac) from dmp_db.base_user_tags_v3 where day='2019-08-19'")
  }
}
