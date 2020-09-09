package top.zhangchao.market_analysis

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import top.zhangchao.bean.MarketingUserBehavior
import top.zhangchao.source.AppMarketingByChannelSource

/**
 * <p>Title: </p>
 *
 * <p>Description: </p>
 *
 * @author Zhang Chao
 * @version java_day
 * @date 2020/9/8 10:00 上午
 */
object AppMarketingStatistics {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(12)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val dataStream: DataStream[MarketingUserBehavior] = env.addSource(new AppMarketingByChannelSource)
        .assignAscendingTimestamps(_.timestamp)

//    dataStream
//      .filter(_.behavior != "UNINSTALL")
//        .

    dataStream.print()

    env.execute()
  }
}
