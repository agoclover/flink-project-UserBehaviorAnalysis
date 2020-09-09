package top.zhangchao.page_ad_analysis

import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import top.zhangchao.bean.{AdClickBlackListWarning, AdClickLog}
import java.net.URL
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector


/**
 * <p>Title: </p>
 *
 * <p>Description: </p>
 *
 * @author Zhang Chao
 * @version java_day
 * @date 2020/9/8 10:43 上午
 */
object AdStatisticsByGeo {
  /*
  异常行为: 短时间内大量刷单, 比如一天之内 (到一天结束的时候要把状态清零)
  通过侧输出流
   */
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime) // 不考虑乱序数据会绝对快

    // 读取数据并转换为样例类

//    val resource: URL = getClass.getResource("/AdClickLog.csv")

    val adlogStream: DataStream[AdClickLog] = env.readTextFile("/Users/amos/Learning/UserBehaviorAnalysis/MarketAnalysis/src/main/resources/AdClickLog.csv")
      .map(line => {
        val arr: Array[String] = line.split(",")
        AdClickLog(arr(0).toLong, arr(1).toLong, arr(2), arr(3), arr(4).toLong)
      })
      .assignAscendingTimestamps(_.timestamp * 1000L)

    val filteredStream: DataStream[AdClickLog] = adlogStream // 主流原封不动
      .keyBy(data => (data.userId, data.adId)) // 同一个用户对同一个广告 的点击行为
      .process(new FilterBlackListUser(100))

    filteredStream.getSideOutput(new OutputTag[AdClickBlackListWarning]("black-list")).print("black list:")

    env.execute()
  }
}

class FilterBlackListUser(maxCount: Long)
  extends KeyedProcessFunction[(Long, Long), AdClickLog, AdClickLog] {

  // 保存当前用户对当前广告的点击量
  lazy val countState: ValueState[Long] = getRuntimeContext.getState(new
      ValueStateDescriptor[Long]("count", classOf[Long]))

  // 标记当前 (用户, 广告) 作为 key 是否第一次发送到黑名单 (是否输出到黑名单的标识位)
  lazy val isInBlackListState: ValueState[Boolean] = getRuntimeContext.getState(new
      ValueStateDescriptor[Boolean]("first-sent-state", classOf[Boolean]))

  // 保存定时器触发的时间戳, 届时清空重置状态
  lazy val resetTime: ValueState[Long] = getRuntimeContext.getState(new
      ValueStateDescriptor[Long]("reset-time-state", classOf[Long]))


  override def processElement(value: AdClickLog,
                              ctx: KeyedProcessFunction[(Long, Long), AdClickLog, AdClickLog]#Context,
                              out: Collector[AdClickLog]): Unit = {
    val curCount: Long = countState.value()

    // 判断环是否是当天第一个数据, 如果是, 则注册第二天零点的定时器
    if (curCount == 0){
      // 计算明天零点的毫秒数
      val ts: Long = (ctx.timerService().currentProcessingTime()/(1000*60*60*24) + 1) * (1000*60*60*24) - (8*1000*60*60)
      resetTime.update(ts)
      ctx.timerService().registerProcessingTimeTimer(ts)
    }

    // 判断是否达到了上限, 符合条件侧输出, 并过滤
    if (curCount > maxCount) {
      // 没有在黑名单里面, 那么输出到侧输出流黑名单信息中
      if (!isInBlackListState.value()){
        ctx.output(new OutputTag[AdClickBlackListWarning]("black-list"),
          AdClickBlackListWarning(value.userId, value.adId, s"Click over $maxCount times today! "))
        isInBlackListState.update(true) // update state
      }
      return
    }
    out.collect(value)
    countState.update(curCount+1)
  }

  override def onTimer(timestamp: Long,
                       ctx: KeyedProcessFunction[(Long, Long),
                         AdClickLog, AdClickLog]#OnTimerContext,
                       out: Collector[AdClickLog]): Unit = {
    // timestamp 是当前时间
    if (timestamp == resetTime.value()){
      countState.clear()
      isInBlackListState.clear()
      resetTime.clear()
    }
  }
}
