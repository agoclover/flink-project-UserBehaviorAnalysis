package top.zhangchao.bean

/**
 * <p>输出统计数据的样例类</p>
 *
 * @author Zhang Chao
 * @version java_day
 * @date 2020/9/8 9:19 上午
 */
case class MarketingViewCount(windowStart: String,
                              windowEnd:String,
                              channel:String,
                              behavior:String,
                              count: Long)
