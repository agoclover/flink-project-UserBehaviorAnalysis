package top.zhangchao.hotpage

import org.apache.flink.streaming.api.TimeCharacteristic
import top.zhangchao.bean.{UserBehavior, UvCount}
import org.apache.flink.streaming.api.scala._
import java.net.URL
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.scala.function.AllWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
/**
 * <p>Title: </p>
 *
 * <p>Description: </p>
 *
 * @author Zhang Chao
 * @version java_day
 * @date 2020/9/7 1:56 下午
 */
object UniqueVisitor {
  def main(args: Array[String]): Unit = {
    // create Execution Env
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    // set time characteristic as event time
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    // 为了打印到控制台的结果不乱序, 我们配置全局的并发为 1, 这里改变并发对结果正确性没有影响
    env.setParallelism(1)

    val url: URL = getClass.getResource("/UserBehaviorTest.csv")

    val dataStream: DataStream[UserBehavior] = env
      .readTextFile(url.getPath)
      .map(line => {
        val lineArr: Array[String] = line.split(",")
        UserBehavior(lineArr(0).toLong, lineArr(1).toLong, lineArr(2).toInt, lineArr(3), lineArr(4).toLong)
      })
      .assignAscendingTimestamps(_.timestamp * 1000) // 指定时间戳和 watermark

    val uvStream: DataStream[UvCount] = dataStream
      .filter(_.behavior == "pv")
      .timeWindowAll(Time.hours(1)) // 所有数据分到同一个组里
//      .apply(new UvCountResult())
        .aggregate(new UvCountAgg(), new UvWindowFunction())

    uvStream.print()

    env.execute()
  }
}

class UvCountResult() extends AllWindowFunction[UserBehavior, UvCount, TimeWindow] {
  override def apply(window: TimeWindow,
                     input: Iterable[UserBehavior],
                     out: Collector[UvCount]): Unit = {

    var idSet: Set[Long] = Set[Long]()

    for(userBehavior <- input){
      idSet += userBehavior.userId

    }
    out.collect(UvCount(window.getEnd, idSet.size))
  }
}

class UvCountAgg() extends AggregateFunction[UserBehavior, Set[Long], Long] {
  override def createAccumulator(): Set[Long] = Set[Long]()

  override def add(value: UserBehavior, accumulator: Set[Long]): Set[Long] = accumulator + value.userId // 返回新的 Set

  override def getResult(accumulator: Set[Long]): Long = accumulator.size

  override def merge(a: Set[Long], b: Set[Long]): Set[Long] = a ++ b // 返回新的 Set
}

class UvWindowFunction() extends AllWindowFunction[Long, UvCount, TimeWindow] {
  override def apply(window: TimeWindow,
                     input: Iterable[Long],
                     out: Collector[UvCount]): Unit =
    out.collect(UvCount(window.getEnd, input.head))
}


