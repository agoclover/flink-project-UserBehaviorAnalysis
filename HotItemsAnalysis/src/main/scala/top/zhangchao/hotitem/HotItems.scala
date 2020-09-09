package top.zhangchao.hotitem

import java.lang
import java.sql.Timestamp
import java.util.Properties

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.api.java.tuple.{Tuple, Tuple1}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.util.Collector
import top.zhangchao.bean.{ItemViewCount, UserBehavior}

/**
 * <p>Title: </p>
 *
 * <p>Description: </p>
 *
 * @author Zhang Chao
 * @version java_day
 * @date 2020/9/5 2:04 下午
 */
object HotItems {
  def main(args: Array[String]): Unit = {
    // create Execution Env
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    // set time characteristic as event time
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    // 为了打印到控制台的结果不乱序, 我们配置全局的并发为 1, 这里改变并发对结果正确性没有影响
    env.setParallelism(1)

//    val fileInputPath = "/Users/amos/Learning/UserBehaviorAnalysis/HotItemsAnalysis/src/main/resources/UserBehavior.csv"
//
//    val userBehaviorStream: DataStream[UserBehavior] = env
//      .readTextFile(fileInputPath)
//      .map(line => {
//        val lineArr: Array[String] = line.split(",")
//        UserBehavior(lineArr(0).toLong, lineArr(1).toLong, lineArr(2).toInt, lineArr(3), lineArr(4).toLong)
//      })
//      .assignAscendingTimestamps(_.timestamp * 1000) // 指定时间戳和 watermark

    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "hadoop102:9092")
    properties.setProperty("group.id", "consumer-group")
    properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("auto.offset.reset", "latest")

    val userBehaviorKafkaStream: DataStream[UserBehavior] = env
      .addSource(new FlinkKafkaConsumer[String]("hotitems", new SimpleStringSchema(), properties))
      .map(line => {
        val lineArr: Array[String] = line.split(",")
        UserBehavior(lineArr(0).toLong, lineArr(1).toLong, lineArr(2).toInt, lineArr(3), lineArr(4).toLong)
      })
      .assignAscendingTimestamps(_.timestamp * 1000) // 指定时间戳和 watermark

    val aggStream: DataStream[ItemViewCount] = userBehaviorKafkaStream
      .filter(_.behavior == "pv")
      .keyBy("itemId") // _.itemId 则传入函数, 后面 key 就是 Long
      .timeWindow(Time.minutes(60), Time.minutes(5))
      .aggregate(new CountAgg(), new WindowResultFunction())

//    aggStream.print()

    // 按照窗口分组, 进行排序并输出 TopN
    val resultStream: DataStream[String] = aggStream
        .keyBy("windowEnd")
        .process(new TopNHotItemsResult(5))

    resultStream.print()

    env.execute()
  }
}

class CountAgg() extends AggregateFunction[UserBehavior, Long, Long] {
  override def createAccumulator(): Long = 0L

  override def add(value: UserBehavior, accumulator: Long): Long = accumulator + 1

  override def getResult(accumulator: Long): Long = accumulator

  override def merge(a: Long, b: Long): Long = a + b
}

class WindowResultFunction extends WindowFunction[Long, ItemViewCount, Tuple, TimeWindow] {
  override def apply(key: Tuple, window: TimeWindow, aggregateResult: Iterable[Long],
                     collector: Collector[ItemViewCount]) : Unit = {
    val itemId: Long = key.asInstanceOf[Tuple1[Long]].f0
    val count = aggregateResult.iterator.next
    collector.collect(ItemViewCount(itemId, window.getEnd, count))
  }
}

class TopNHotItemsResult(n:Int) extends KeyedProcessFunction[Tuple, ItemViewCount, String]{

  lazy val listState: ListState[ItemViewCount] = getRuntimeContext.getListState(
    new ListStateDescriptor[ItemViewCount]("itemViewCountList", classOf[ItemViewCount]))

  override def processElement(value: ItemViewCount,
                              ctx: KeyedProcessFunction[Tuple, ItemViewCount, String]#Context,
                              out: Collector[String]): Unit = {
    listState.add(value)

    // 注册一个定时器, windowEnd + 100ms 触发
    ctx.timerService().registerEventTimeTimer(value.windowEnd + 100) // 分组 windowEnd 一样的, 所以就算重复注册也会是一个
  }

  override def onTimer(timestamp: Long,
                       ctx: KeyedProcessFunction[Tuple, ItemViewCount, String]#OnTimerContext,
                       out: Collector[String]): Unit = {
    import scala.collection.JavaConversions._
    val allItemViewCountList: List[ItemViewCount] = listState.get().toList

    listState.clear()
    // sort
    val topNHotItemViewCountList: List[ItemViewCount] = allItemViewCountList
      .sortBy(_.count)(Ordering.Long.reverse)
      .take(n)

    // 排名信息格式化输出
    val result = new StringBuilder
    result.append("窗口结束时间: ").append(new Timestamp(timestamp - 100)).append("\n")

    // 遍历 topN 列表, 逐个输出
    for (i <- topNHotItemViewCountList.indices){
      val currentItemViewCount: ItemViewCount = topNHotItemViewCountList(i)
      result.append("NO.").append(i + 1).append(":")
        .append("\t 商品 ID = ").append(currentItemViewCount.itemId)
        .append("\t 热门度  = ").append(currentItemViewCount.count)
        .append("\n")
    }

    result.append("\n=========\n\n")
    // 控制输出频率
    Thread.sleep(1000)
    out.collect(result.toString())
  }
}
