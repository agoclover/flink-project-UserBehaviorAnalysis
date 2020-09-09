package top.zhangchao.page_ad_analysis

import java.net.URL
import java.sql.Timestamp

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import top.zhangchao.bean.{AdClickLog, AdClickLogCountByProvince}

/**
 * <p>Title: </p>
 *
 * <p>Description: </p>
 *
 * @author Zhang Chao
 * @version java_day
 * @date 2020/9/8 10:18 上午
 */
object AdClickAnalysisByProvince {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime) // 不考虑乱序数据会绝对快

    // 读取数据并转换为样例类

    val resource: URL = getClass.getResource("/AdClickLog.csv")

    val adlogStream: DataStream[AdClickLog] = env.readTextFile(resource.getPath)
      .map(line => {
        val arr: Array[String] = line.split(",")
        AdClickLog(arr(0).toLong, arr(1).toLong, arr(2), arr(3), arr(4).toLong)
      })
      .assignAscendingTimestamps(_.timestamp * 1000L)

    val resultStream: DataStream[AdClickLogCountByProvince] = adlogStream
      .keyBy(_.province)
      .timeWindow(Time.hours(1), Time.seconds(5))
      .aggregate(new AdCountAgg(), new AdCountResult()) // 增量聚合 + WindowFunction

    resultStream.print()

    env.execute()
  }
}

class AdCountAgg() extends AggregateFunction[AdClickLog, Long, Long] {
  override def createAccumulator(): Long = 0L

  override def add(value: AdClickLog, accumulator: Long): Long = accumulator+1

  override def getResult(accumulator: Long): Long = accumulator

  override def merge(a: Long, b: Long): Long = a+b
}

class AdCountResult() extends WindowFunction[Long, AdClickLogCountByProvince, String, TimeWindow] {
  override def apply(key: String,
                     window: TimeWindow,
                     input: Iterable[Long],
                     out: Collector[AdClickLogCountByProvince]): Unit = {
    val end: String = new Timestamp(window.getEnd).toString
    out.collect(AdClickLogCountByProvince(end, key, input.head))
  }
}
