package top.zhangchao.source

import java.util.UUID

import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext
import top.zhangchao.bean.MarketingUserBehavior

import scala.util.Random

/**
 * <p>Title: </p>
 *
 * <p>Description: </p>
 *
 * @author Zhang Chao
 * @version java_day
 * @date 2020/9/8 9:05 上午
 */
class AppMarketingByChannelSource extends RichParallelSourceFunction[MarketingUserBehavior] {

  var running = true
  val channelSet: Seq[String] = Seq("AppStore", "XiaomiStore", "HuaweiStore", "Weibo", "Wechat", "Tieba")
  val behaviorTypes: Seq[String] = Seq("BROWSE", "CLICK", "PURCHASE", "UNINSTALL")
  val rand: Random = Random

  override def run(ctx: SourceContext[MarketingUserBehavior]): Unit = {
    val maxElements: Long = Long.MaxValue
    var count = 0L

    while (running && count < maxElements) {
      val id: String = UUID.randomUUID().toString
      val behaviorType: String = behaviorTypes(rand.nextInt(behaviorTypes.size)) // 做权重
      val channel: String = channelSet(rand.nextInt(channelSet.size))
      val ts:Long = System.currentTimeMillis()

      ctx.collect(MarketingUserBehavior(id, behaviorType, channel, ts))
      count += 1
      Thread.sleep(20)
    }
  }

  override def cancel(): Unit = running = false
}