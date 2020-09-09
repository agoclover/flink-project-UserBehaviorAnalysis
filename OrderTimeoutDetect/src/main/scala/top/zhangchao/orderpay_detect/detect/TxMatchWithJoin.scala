package top.zhangchao.orderpay_detect.detect

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector
import top.zhangchao.orderpay_detect.bean.{OrderEvent, ReceiptEvent}

/**
 * <p>Title: </p>
 *
 * <p>Description: </p>
 *
 * @author Zhang Chao
 * @version java_day
 * @date 2020/9/9 3:52 下午
 */
object TxMatchWithJoin {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val orderEventStream: KeyedStream[OrderEvent, String] = env.readTextFile("/Users/amos/Learning/UserBehaviorAnalysis/OrderTimeoutDetect/src/main/resources/OrderLog.csv")
      .map(line => {
        val arr: Array[String] = line.split(",")
        OrderEvent(arr(0).toLong, arr(1), arr(2), arr(3).toLong)
      })
      .assignAscendingTimestamps(_.timestamp * 1000L)
      .filter(_.txId != "") // create 没有 txID
      .keyBy(_.txId)

    val receiptEventStream: KeyedStream[ReceiptEvent, String] = env.readTextFile("/Users/amos/Learning/UserBehaviorAnalysis/OrderTimeoutDetect/src/main/resources/ReceiptLog.csv")
      .map(line => {
        val arr: Array[String] = line.split(",")
        ReceiptEvent(arr(0), arr(1), arr(2).toLong)
      })
      .assignAscendingTimestamps(_.timestamp * 1000L)
      .keyBy(_.txId)

    // 只能拿到匹配到的事件
    val resultStream: DataStream[(OrderEvent, ReceiptEvent)] = orderEventStream.intervalJoin(receiptEventStream)
      .between(Time.seconds(-3), Time.seconds(5))
      .process(new JoinTwoStream())

    resultStream.print()
    env.execute()
  }
}

class JoinTwoStream() extends ProcessJoinFunction[OrderEvent,ReceiptEvent,(OrderEvent, ReceiptEvent)] {
  override def processElement(left: OrderEvent,
                              right: ReceiptEvent,
                              ctx: ProcessJoinFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]#Context,
                              out: Collector[(OrderEvent, ReceiptEvent)]): Unit = {
    out.collect((left,right))
  }
}

