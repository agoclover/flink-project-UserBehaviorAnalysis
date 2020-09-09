package top.zhangchao.loginfaildetect.detect

import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector
import top.zhangchao.loginfaildetect.bean.{LoginEvent, LoginFailWarning}

/**
 * <p>Title: </p>
 *
 * <p>Description: </p>
 *
 * @author Zhang Chao
 * @version java_day
 * @date 2020/9/8 11:51 上午
 */
object LoginFail {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(12)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val inputStream: DataStream[String] = env.readTextFile("/Users/amos/Learning/UserBehaviorAnalysis/LoginFailDetect/src/main/resources/LoginLog.csv")
    val dataStream: DataStream[LoginEvent] = inputStream.map(line => {
      val arr: Array[String] = line.split(",")
      LoginEvent(arr(0).toLong, arr(1), arr(2), arr(3).toLong)
    })
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[LoginEvent](Time.seconds(3)) {
        override def extractTimestamp(element: LoginEvent) = element.eventTime
      })

    val loginFailWarningStream: DataStream[LoginFailWarning] = dataStream
      .keyBy(_.userId)
      .process(new LoginFailDetectWarning2())

    loginFailWarningStream.print()

    env.execute()
  }
}

/*
这种做法只能隔2秒之后去判断一下这期间是否有多次失败登录，而不是在一次登录失败之后、再一次登录失败时就立刻报警。
这个需求如果严格实现起来，相当于要判断任意紧邻的事件，是否符合某种模式。
 */
class LoginFailDetectWarning(loginFailTimes: Int)
  extends KeyedProcessFunction[Long, LoginEvent, LoginFailWarning] {
  // 定义状态, 保存 2s 内所有登录失败事件的列表, 定时器时间戳
  lazy val loginFailEventListState: ListState[LoginEvent] = getRuntimeContext.getListState(
    new ListStateDescriptor[LoginEvent]("fail-list", classOf[LoginEvent]))

  lazy val timerTsState: ValueState[Long] = getRuntimeContext.getState(
    new ValueStateDescriptor[Long]("ts", classOf[Long])
  )

  override def processElement(value: LoginEvent,
                              ctx: KeyedProcessFunction[Long, LoginEvent, LoginFailWarning]#Context,
                              out: Collector[LoginFailWarning]): Unit = {
    if (value.eventType =="fail"){
      // add to fail list
      loginFailEventListState.add(value)
      // register timer if there is not timer
      if (timerTsState.value() == 0) {
        val ts: Long = value.eventTime * 1000L + 2000L
        ctx.timerService().registerEventTimeTimer(ts)
        timerTsState.update(ts)
      }
    } else {
      // 删除定时器, 清空状态, 重新开始
      ctx.timerService().deleteEventTimeTimer(timerTsState.value())
      // clear state
      loginFailEventListState.clear()
      timerTsState.clear()
    }
  }

  override def onTimer(timestamp: Long,
                       ctx: KeyedProcessFunction[Long, LoginEvent, LoginFailWarning]#OnTimerContext,
                       out: Collector[LoginFailWarning]): Unit = {
    import scala.collection.JavaConversions._
    val loginFailList = loginFailEventListState.get()
    // 定时器触发说明 2 秒内没有成功数据, 判断一共有多少次, loginFailTimes 次以上就算恶意登录
    if (loginFailList.size >= loginFailTimes) {
      out.collect(LoginFailWarning(ctx.getCurrentKey,
        loginFailList.head.eventTime,
        loginFailList.last.eventTime,
      s"login fail in 2s for ${loginFailList.size} times! "))
    }
    // clear the state
    // 这里和以前一样有一点小问题, 业务逻辑的问题
    loginFailEventListState.clear()
    timerTsState.clear()
  }
}

class LoginFailDetectWarning2()
  extends KeyedProcessFunction[Long, LoginEvent, LoginFailWarning] {
  // 定义状态, 保存 2s 内所有登录失败事件的列表, 定时器时间戳
//  lazy val loginFailEventListState: ListState[LoginEvent] = getRuntimeContext.getListState(
//    new ListStateDescriptor[LoginEvent]("fail-list", classOf[LoginEvent]))

  lazy val timerTsState: ValueState[Long] = getRuntimeContext.getState(
    new ValueStateDescriptor[Long]("ts", classOf[Long]))

  lazy val loginFailState: ValueState[LoginEvent] = getRuntimeContext.getState(
    new ValueStateDescriptor[LoginEvent]("last-event", classOf[LoginEvent])
  )

  override def processElement(value: LoginEvent,
                              ctx: KeyedProcessFunction[Long, LoginEvent, LoginFailWarning]#Context,
                              out: Collector[LoginFailWarning]): Unit = {
    if (value.eventType == "fail"){
      if(loginFailState.value() != null){
        val lastLoginEvent: LoginEvent = loginFailState.value()
        val lastTime: Long = timerTsState.value()

        if (lastLoginEvent.eventType == "fail" && (value.eventTime - lastTime)<=2000L) {
          out.collect(LoginFailWarning(ctx.getCurrentKey, lastTime, value.eventTime, "fail"))
        }
      }
      loginFailState.update(value)
      timerTsState.update(value.eventTime)
    } else {
      // clear state
      loginFailState.clear()
      timerTsState.clear()
    }
  }
}

