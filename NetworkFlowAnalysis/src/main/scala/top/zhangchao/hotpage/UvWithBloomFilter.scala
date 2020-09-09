package top.zhangchao.hotpage

import org.apache.flink.streaming.api.TimeCharacteristic
import top.zhangchao.bean.{UserBehavior, UvCount}
import org.apache.flink.streaming.api.scala._
import java.net.URL
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import redis.clients.jedis.Jedis

/**
 * <p>Title: </p>
 *
 * <p>Description: </p>
 *
 * @author Zhang Chao
 * @version java_day
 * @date 2020/9/7 3:02 下午
 */
object UvWithBloomFilter {
  def main(args: Array[String]): Unit = {
    // create Execution Env
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    // set time characteristic as event time
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    // 为了打印到控制台的结果不乱序, 我们配置全局的并发为 1, 这里改变并发对结果正确性没有影响
    env.setParallelism(1)

    val url: URL = getClass.getResource("/UserBehavior.csv")

    val dataStream: DataStream[UserBehavior] = env
      .readTextFile(url.getPath)
      .map(line => {
        val lineArr: Array[String] = line.split(",")
        UserBehavior(lineArr(0).toLong, lineArr(1).toLong, lineArr(2).toInt, lineArr(3), lineArr(4).toLong)
      })
      .assignAscendingTimestamps(_.timestamp * 1000) // 指定时间戳和 watermark

    val uvStream = dataStream
      .filter(_.behavior == "pv")
      .map(data => ("uv", data.userId)) // map 成 二元组, 只需要 userId 和 dummy key
      .keyBy(_._1)
      .timeWindow(Time.hours(1))
      .trigger(new MyTrigger()) // 自定义窗口触发规则
      .process(new UvCountResultWithBloom())

    uvStream.print()

    env.execute()
  }
}

/*
自定义触发器: 每个 element 来了之后都触发一次窗口计算
 */
class MyTrigger() extends Trigger[(String, Long), TimeWindow] {
  override def onElement(element: (String, Long),
                         timestamp: Long,
                         window: TimeWindow,
                         ctx: Trigger.TriggerContext): TriggerResult = TriggerResult.FIRE_AND_PURGE

  override def onProcessingTime(time: Long,
                                window: TimeWindow,
                                ctx: Trigger.TriggerContext): TriggerResult = TriggerResult.CONTINUE

  override def onEventTime(time: Long,
                           window: TimeWindow,
                           ctx: Trigger.TriggerContext): TriggerResult = TriggerResult.CONTINUE

  override def clear(window: TimeWindow, ctx: Trigger.TriggerContext): Unit = {}
}


class MyBloomFilter(size: Long) extends Serializable{
  // 指定布隆过滤器的位图大小由外部参数指定, 位图存在 redis 中
  private val cap = size // 最好字节的整倍数或 2 的整倍数, 1000,0000

  // 实现一个 hash 函数
  def hash(value: String, seed: Int):Long = { // murmur3
    var result = 0L
    for(i <- 0 until value.length){
      result = result * seed + value.charAt(i)
    }
    // 返回 hash 值, 截取在范围内的位数
    (cap - 1) & result
  }
}

/**
 * 自定义 ProcessFunction, 实现对于每个 userId 的去重判断.
 */
class UvCountResultWithBloom() extends ProcessWindowFunction[(String, Long), UvCount, String,TimeWindow] {

  lazy val jedis = new Jedis("127.0.0.1", 6379)
  private val bloom = new MyBloomFilter(1 << 29) // 1 亿位 需要 2^27, 4 bit 设置成以一个, 那么就是 2^29
  /*
  1 亿用户数据怎么存储?

  如果使用 布隆过滤器 的话, 1 亿数据全部按照 bit 来存储, 则需要的内存大小如下:

    2^10 = 1024 sim 10^3,
    1 亿 = 10^8 = (2^10)^2 * 100 = 2^20 * 2^7
    如果算 4 位一个数据, 则 2^27 *4 = 2^29
    2^29 bit = 2^26 Byte = 2^6 MB = 64MB
    即使用 布隆过滤器 的话, 按照 4 位一个 id 排布, 1 亿数据全部按照 bit 来存储, 需要 64 MB 的内存.

  普通存储?

    一个用户算 10 个字节, 即 10 * 8 / 4 = 20, 即 64 * 20 MB = 1280 MB 即约 1G 的内存

  Redis 底层以 16 进制存储: \x00 \x01

   */
  override def process(key: String,
                       context: Context,
                       elements: Iterable[(String, Long)],
                       out: Collector[UvCount]): Unit = {
    // 每来一个数, 将其 userId 进行 hash, 到 redis 位图中判断是否存在
    val bitmapKey:String = context.window.getEnd.toString // 以 windowEnd 作为位图的 key

    val userId:String = elements.last._2.toString
    val offset:Long = bloom.hash(userId, 61) // 调用 hash 函数, 计算位图中的偏移量


    val isExist:Boolean = jedis.getbit(bitmapKey, offset) // 调用 redis 位命令, 得到是否存在的结果

    // 如果存在, 什么都不做, 如果不存在, 将对应位置 , count 值加 1
    // 因为窗口状态要清空, 所以要将 count 值保存到 redis 中
    val  countMap = "uvCount" // 所有窗口的 uvCount 值保存成一个 hashmap
    val uvCountKey = bitmapKey // 每个窗口的 uv count key 就是 windowEnd
    var count = 0l

    //先取出 redis 中的状态
    val str: String = jedis.hget(countMap, uvCountKey)
    if( str != null){
      count = str.toLong
    }
    // 存在则不用管
    if (!isExist){
      /*
      设定的 过程就就是不断增加的过程
       */
      jedis.setbit(bitmapKey, offset, true)
      // 更新状态
      jedis.hset(countMap, uvCountKey, (count+1).toString)
      out.collect(UvCount(uvCountKey.toLong, count+1))
    }
  }
}