package top.zhangchao.loginfaildetect.detect

import java.util

import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.cep.PatternSelectFunction
import org.apache.flink.cep.scala.{CEP, PatternStream}
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector
import top.zhangchao.loginfaildetect.bean.{LoginEvent, LoginFailWarning}

import scala.collection.mutable.ListBuffer

/**
 * <p>Title: </p>
 *
 * <p>Description: </p>
 *
 * @author Zhang Chao
 * @version java_day
 * @date 2020/9/8 3:31 下午
 */
object LoginFailWithCEP {
  val LOGIN_FAIL_TIME:Int = 2
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

    // 2. 类似于模式匹配的 匹配模式, 用来检测复杂事件序列
//    val loginFailPattern: Pattern[LoginEvent, LoginEvent] = Pattern
//      .begin[LoginEvent]("firstFail").where(_.eventType == "fail")
//      .next("secondFail").where(_.eventType == "fail")
//      .next("thirdFail").where(_.eventType == "fail")
//      .within(Time.seconds(5))

    val loginFailPattern: Pattern[LoginEvent, LoginEvent] = Pattern
      .begin[LoginEvent]("fail").where(_.eventType == "fail").times(LOGIN_FAIL_TIME).consecutive() // 如果只有 .times(), 默认的是 followedBy, 宽松近邻检测; 需要添加 .consecutive
      .within(Time.seconds(5))


    // 3. 对数据流应用定义好的模式, 得到 PatternStream
    val patternStream: PatternStream[LoginEvent] = CEP.pattern(dataStream.keyBy(_.userId), loginFailPattern)

    // 4. 检出符合匹配条件的 事件序列 (一个新的流), 做转换输出
    val loginFailDataStream: DataStream[LoginFailWarning] = patternStream.select(new LoginFailSelect())

    loginFailDataStream.print()

    env.execute()
  }

}

class LoginFailSelect() extends PatternSelectFunction[LoginEvent, LoginFailWarning] {
  override def select(map: util.Map[String, util.List[LoginEvent]]): LoginFailWarning = {
//    // 从 map 结构中可以拿到第一次和第二次登陆失败的事件
//    val firstFailEvent: LoginEvent = map.get("firstFail").get(0)
//    val secondFailEvent: LoginEvent = map.get("thirdFail").get(0)
//    LoginFailWarning(firstFailEvent.userId, firstFailEvent.eventTime, secondFailEvent.eventTime, "fail")

    // 从 map 结构中可以拿到第一次和第二次登陆失败的事件
    import scala.collection.JavaConversions._
    val failEventList: List[LoginEvent] = map.get("fail").iterator().toList
    LoginFailWarning(failEventList.head.userId, failEventList.head.eventTime, failEventList.last.eventTime, "fail")
  }
}
