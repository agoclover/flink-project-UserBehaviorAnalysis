package top.zhangchao.market_analysis


import java.sql.Timestamp

import org.apache.flink.api.java.tuple.{Tuple, Tuple2}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import top.zhangchao.bean.{MarketingUserBehavior, MarketingViewCount}
import top.zhangchao.source.AppMarketingByChannelSource

/**
 * <p>Title: </p>
 *
 * <p>Description: </p>
 *
 * @author Zhang Chao
 * @version java_day
 * @date 2020/9/8 9:20 上午
 */
object AppMarketingByChannel {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(12)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val dataStream: DataStream[MarketingUserBehavior] = env.addSource(new AppMarketingByChannelSource)
      .assignAscendingTimestamps(_.timestamp) //mm

    val resultStream: DataStream[MarketingViewCount] = dataStream
      .filter(_.behavior != "UNINSTALL")
      .keyBy("channel", "behavior")  //key tuple
//      .keyBy(2, 1) // by indices
//      .keyBy(data => (data.channel, data.behavior)) // lambda phrase
      .timeWindow(Time.hours(1), Time.seconds(5)) // 可以 aggregate()之前的, 增量聚合 全窗口函数
      .process(new MarketingCountByChannel())

    resultStream.print()
    env.execute()
  }
}

// 实现自定义的 processWindowFunctinon
class MarketingCountByChannel() extends ProcessWindowFunction[MarketingUserBehavior, MarketingViewCount, Tuple, TimeWindow] {
  override def process(key: Tuple,
                       context: Context,
                       elements: Iterable[MarketingUserBehavior],
                       out: Collector[MarketingViewCount]): Unit = {
    val start: String = new Timestamp(context.window.getStart).toString
    val end: String = new Timestamp(context.window.getEnd).toString
    val channel: String = key.asInstanceOf[Tuple2[String, String]].f0
    val behavior: String = key.asInstanceOf[Tuple2[String, String]].f1
    val count: Int = elements.size
    out.collect(MarketingViewCount(start, end, channel, behavior, count))
  }
}

