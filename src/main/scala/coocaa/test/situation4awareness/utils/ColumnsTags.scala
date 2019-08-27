package coocaa.test.situation4awareness.utils

/**
  * Created by Joe.Kwan on 2018/11/7 10:33.
  */
object ColumnsTags {

  case class StatsEdwPlayerTest(mac: String,
                                did: String,
                                province: String,
                                city: String,
                                source: String,
                                start_time: String,
                                stop_time: String,
                                dur: Long,
                                video_id: String,
                                name: String,
                                category: String,
                                video_source: String,
                                partition_day: String
                               )




  case class StatsOdsCcVideo2018(coocaa_v_id: String,
                                 tag: String)



  case class StatsEdwPlayerTest2(mac: String,
                                 did: String,
                                 province: String,
                                 city: String,
                                 source: String,
                                 start_time: String,
                                 stop_time: String,
                                 dur: Long,
                                 video_id: String,
                                 name: String,
                                 category: String,
                                 video_source: String,
                                 partition_day: String,
                                 tag: String)

  case class StatsClickOut(video_id: String,
                           name: String,
                           category: String,
                           partition_day: String,
                           sum_dur_hours: Long,
                           count_numbers: Long)

  case class StatsTagOut(single_tag: String,
                         category: String,
                         partition_day: String,
                         sum_dur_hours: Long,
                         count_numbers: Long)

}
