package top.zhangchao.orderpay_detect.detect

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.co.{KeyedCoProcessFunction}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector
import top.zhangchao.orderpay_detect.bean.{OrderEvent, ReceiptEvent}

/**
 * <p>Title: </p>
 *
 * <p>Description: </p>
 *
 * @author Zhang Chao
 * @version java_day
 * @date 2020/9/9 2:28 下午
 */
object TxMatch {
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
      .filter(_.txId != "") // create 没有 txID
    val receiptEventStream: DataStream[ReceiptEvent] = env.readTextFile("/Users/amos/Learning/UserBehaviorAnalysis/OrderTimeoutDetect/src/main/resources/ReceiptLog.csv")
      .map(line => {
        val arr: Array[String] = line.split(",")
        ReceiptEvent(arr(0), arr(1), arr(2).toLong)
      })
      .assignAscendingTimestamps(_.timestamp * 1000L)

    // connect 类型一样, union 必须类型一样, 可以多条流
    // connect to dataSream to handle. 也可以两条 keyedStream 进行 connect
    val resultStream: DataStream[(OrderEvent, ReceiptEvent)] = orderEventStream.connect(receiptEventStream)
      .keyBy(_.txId, _.txId)
      .process(new TxMatchDetect())

    resultStream.print("joined")
    resultStream.getSideOutput(new OutputTag[OrderEvent]("single-pay")).print("single-pay")
    resultStream.getSideOutput(new OutputTag[ReceiptEvent]("single-receipt")).print("single-receipt")

    env.execute()
  }
}

class TxMatchDetect() extends KeyedCoProcessFunction[String, OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)] {


  lazy val payEventState: ValueState[OrderEvent] = getRuntimeContext.getState(new ValueStateDescriptor[OrderEvent]("pay", classOf[OrderEvent]))
  lazy val receiptEventState: ValueState[ReceiptEvent] = getRuntimeContext.getState(new ValueStateDescriptor[ReceiptEvent]("receipt", classOf[ReceiptEvent]))

  override def processElement1(pay: OrderEvent,
                               ctx: KeyedCoProcessFunction[String, OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]#Context,
                               out: Collector[(OrderEvent, ReceiptEvent)]): Unit = {
    val receipt: ReceiptEvent = receiptEventState.value()
    if (receipt != null) {
      out.collect((pay,receipt))
      payEventState.clear()
      receiptEventState.clear() // clear 掉, 防止内存溢出
    } else {
      // 注册定时器, 等待 receipt
      payEventState.update(pay)
      ctx.timerService().registerEventTimeTimer(pay.timestamp * 1000L + 5000L)
    }
  }

  override def processElement2(receipt: ReceiptEvent,
                               ctx: KeyedCoProcessFunction[String, OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]#Context,
                               out: Collector[(OrderEvent, ReceiptEvent)]): Unit = {
    val pay: OrderEvent = payEventState.value()
    if (pay != null) {
      out.collect((pay,receipt))
      payEventState.clear()
      receiptEventState.clear()
    } else {
      // 注册定时器, 等待 receipt
      receiptEventState.update(receipt)
      ctx.timerService().registerEventTimeTimer(receipt.timestamp * 1000L + 3000L)
    }
  }

  override def onTimer(timestamp: Long,
                       ctx: KeyedCoProcessFunction[String, OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]#OnTimerContext,
                       out: Collector[(OrderEvent, ReceiptEvent)]): Unit = {
    if (payEventState.value() != null){
      ctx.output(new OutputTag[OrderEvent]("single-pay"), payEventState.value())
    }
    if (receiptEventState.value() != null){
      ctx.output(new OutputTag[ReceiptEvent]("single-receipt"), receiptEventState.value())
    }
  }
}
