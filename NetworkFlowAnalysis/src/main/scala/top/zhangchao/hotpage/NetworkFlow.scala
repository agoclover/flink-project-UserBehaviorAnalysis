package top.zhangchao.hotpage

import java.util
import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.Map
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, MapState, MapStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import top.zhangchao.bean.{ApacheLogEvent, UrlViewCount}
import scala.collection.mutable.ListBuffer
import scala.util.matching.Regex

/**
 * <p>Title: </p>
 *
 * <p>Description: </p>
 *
 * @author Zhang Chao
 * @version java_day
 * @date 2020/9/7 8:55 上午
 */
object NetworkFlow {
  def main(args: Array[String]): Unit = {
    // create Execution Env
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    // set time characteristic as event time
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    // 为了打印到控制台的结果不乱序, 我们配置全局的并发为 1, 这里改变并发对结果正确性没有影响
    env.setParallelism(1)

    val fileInputPath = "/Users/amos/Learning/UserBehaviorAnalysis/NetworkFlowAnalysis/src/main/resources/apache.log"

    val apacheLogStream: DataStream[ApacheLogEvent] = env
      .readTextFile(fileInputPath)
      .map(line => {
        val lineArr: Array[String] = line.split(" ")
        val sdf = new SimpleDateFormat("dd/MM/yyyy:HH:m:ss")
        val timestamp: Long = sdf.parse(lineArr(3)).getTime
        ApacheLogEvent(lineArr(0), lineArr(2), timestamp, lineArr(5), lineArr(6))
      })
      .assignTimestampsAndWatermarks(new
          BoundedOutOfOrdernessTimestampExtractor[ApacheLogEvent](Time.seconds(3)) {
        override def extractTimestamp(element: ApacheLogEvent) = element.eventTime
      })

    val lateTag = new OutputTag[ApacheLogEvent]("lateTag")

    val tempStream: WindowedStream[ApacheLogEvent, String, TimeWindow] = apacheLogStream
      .filter(data => {
        val pattern: Regex = "^((?!\\.(css|js)$).)*$".r
        val maybeString: Option[String] = pattern.findFirstIn(data.url)
        maybeString.nonEmpty
      })
      .filter(_.method == "GET")
      .keyBy(_.url)
      .timeWindow(Time.minutes(10), Time.seconds(5))
      .allowedLateness(Time.minutes(1))
      .sideOutputLateData(lateTag)

    val aggStream: DataStream[UrlViewCount] = tempStream
      .aggregate(new PageCountAgg(), new PageCountWindowResult())

    val resultStream: DataStream[String] = aggStream
      .keyBy(_.windowEnd)
      .process(new TopNPageResult(3))

    val lateResultStream: DataStream[ApacheLogEvent] = aggStream.getSideOutput(lateTag)

    resultStream.print("result: ")
    /*
    如果是滚动窗口, 那么侧输出流会直接输出, 但是滑动窗口的话, 只要这个数据有处于某个窗口, 就不会被输出到侧输出流. 与 watermark相比
    所以, 比如 10:25:50 的数据, 在 10:15-10:25:50 中, 就不会输出, 会变成侧输出流
     */
    lateResultStream.print("late result: ")

    env.execute()
  }
}

class PageCountAgg extends AggregateFunction[ApacheLogEvent, Long, Long]() {
  override def createAccumulator(): Long = 0L

  override def add(value: ApacheLogEvent, accumulator: Long): Long = accumulator + 1

  override def getResult(accumulator: Long): Long = accumulator

  override def merge(a: Long, b: Long): Long = a+b
}

class PageCountWindowResult extends WindowFunction[Long,UrlViewCount, String, TimeWindow]{
  override def apply(key: String,
                     window: TimeWindow,
                     input: Iterable[Long],
                     out: Collector[UrlViewCount]): Unit = {
    out.collect(UrlViewCount(key, window.getEnd, input.head))
  }
}

// 实现自定义排序输出
class TopNPageResult(n:Int) extends KeyedProcessFunction[Long, UrlViewCount, String] {
  // 为了对同一个 key 进行更新操作, 定义更新操作
  lazy private val mapState:MapState[String, Long] = getRuntimeContext.getMapState(
    new MapStateDescriptor[String, Long]("topN-Map", classOf[String], classOf[Long]))

  override def processElement(value: UrlViewCount,
                              ctx: KeyedProcessFunction[Long, UrlViewCount, String]#Context,
                              out: Collector[String]): Unit = {
    mapState.put(value.url, value.count)
    /*
    如果是 list, onTimer里 clear, 那么迟到的每一个样例类相当于被添加到一个保有状态的 list;
    如果是 list, 这里直接 clear, 那么迟到的每一个样例类相当于被添加到一个空 list, 但是迟到数据每次是一个样例类, 它的 url 在这个 list 里其实已经有了, 所以尽管相应的 url 的 count 已经更新, 但是 list 没有去重功能, 所以就重复了;
    所以只能是 set 或 map, 但是没有 MapState, 所以用 MapState, 换个泛型, 原来是样例类, 换成 (String, Long) 就好了.
     */
    ctx.timerService().registerEventTimeTimer(value.windowEnd + 100)
    ctx.timerService().registerEventTimeTimer(value.windowEnd + 60 * 1000L)
  }

  override def onTimer(timestamp: Long,
                       ctx: KeyedProcessFunction[Long, UrlViewCount, String]#OnTimerContext,
                       out: Collector[String]): Unit = {
    // 乱序数据的调整
    if (timestamp == ctx.getCurrentKey + 60 * 1000L) {
      mapState.clear()
      return
    }

    val allUrlViewCounts: ListBuffer[(String, Long)] = ListBuffer()
    val iter: util.Iterator[Map.Entry[String, Long]] = mapState.entries().iterator()

    while (iter.hasNext){
      val value: Map.Entry[String, Long] = iter.next()
      allUrlViewCounts.append((value.getKey, value.getValue()))
    }

    // 排序取 topN
    val topNTopUrlViewCounts: ListBuffer[(String, Long)] = allUrlViewCounts
      .sortWith(_._2 > _._2)
      .take(n)

    // 排名信息格式化输出
    val result = new StringBuilder
    result.append("窗口结束时间: ").append(new Timestamp(timestamp - 100)).append("\n")

    // 遍历 topN 列表, 逐个输出
    for (i <- topNTopUrlViewCounts.indices){
      val currentUrlViewCount: (String, Long) = topNTopUrlViewCounts(i)
      result.append("NO.").append(i + 1).append(":")
        .append("\t Url ID = ").append(currentUrlViewCount._1)
        .append("\t 热门度  = ").append(currentUrlViewCount._2)
        .append("\n")
    }

    result.append("\n=========\n\n")
    // 控制输出频率
    Thread.sleep(1000)
    out.collect(result.toString())
  }
}


