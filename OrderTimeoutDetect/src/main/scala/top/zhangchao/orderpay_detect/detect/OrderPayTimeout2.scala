package top.zhangchao.orderpay_detect.detect

import java.util

import org.apache.flink.cep.{PatternSelectFunction, PatternTimeoutFunction}
import org.apache.flink.cep.scala.{CEP, PatternStream}
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import top.zhangchao.orderpay_detect.bean.{OrderEvent, OrderPayResult}

/**
 * <p>Title: </p>
 *
 * <p>正则 NFA </p>
 *
 * @author Zhang Chao
 * @version java_day
 * @date 2020/9/9 9:17 上午
 */
object OrderPayTimeout2 {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val orderEventStream: DataStream[OrderEvent] = env.readTextFile("/Users/amos/Learning/UserBehaviorAnalysis/OrderTimeoutDetect/src/main/resources/OrderLog.csv")
      .map(line => {
        val arr: Array[String] = line.split(",")
        OrderEvent(arr(0).toLong, arr(1), arr(2), arr(3).toLong)
      })
      .assignAscendingTimestamps(_.timestamp * 1000L)

    // 1. 定义一个匹配模式
    val orderPayPattern: Pattern[OrderEvent, OrderEvent] = Pattern
      .begin[OrderEvent]("create").where(_.eventType == "create")
      .followedBy("pay").where(_.eventType == "pay")
      .within(Time.minutes(15))

    // 2. 将 pattern 应用在数据流上, 进行复杂事件序列的检测
    val patternStream: PatternStream[OrderEvent] = CEP.pattern(orderEventStream.keyBy(_.orderId), orderPayPattern)

    // 3. 定义一个侧输出流标签,

    val timeoutOutputTag = new OutputTag[OrderPayResult]("timeout")

    // 3. 检出复杂事件, 并转换输出结果
    val resultStream: DataStream[OrderPayResult] = patternStream.select(
      timeoutOutputTag,
      new PatternPayTimeoutSelect(),
      new OrderPaySelectMain())


    resultStream.print("payed: ")
    resultStream.getSideOutput(timeoutOutputTag).print("timeout: ")
    env.execute()
  }
}

class OrderPaySelectMain() extends PatternSelectFunction[OrderEvent, OrderPayResult] {
  override def select(pattern: util.Map[String, util.List[OrderEvent]]): OrderPayResult = {
    val payedOrderId = pattern.get("pay").iterator().next().orderId
    OrderPayResult(payedOrderId, "payed order")
  }
}

class PatternPayTimeoutSelect() extends  PatternTimeoutFunction[OrderEvent, OrderPayResult] {
  override def timeout(pattern: util.Map[String, util.List[OrderEvent]],
                       timeoutTimestamp: Long): OrderPayResult = {// 相当于按照超时时间定义了一个定时器
    val timeoutOrderId = pattern.get("create").iterator().next().orderId
    OrderPayResult(timeoutOrderId, s"order timeout at $timeoutTimestamp")

  }
}