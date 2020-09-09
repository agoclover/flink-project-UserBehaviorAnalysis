package top.zhangchao.bean

/**
 * <p>Apache Log</p>
 *
 * <p>Description: </p>
 *
 * @author Zhang Chao
 * @version java_day
 * @date 2020/9/7 8:58 上午
 */
case class ApacheLogEvent(ip: String,
                          userId: String,
                          eventTime: Long,
                          method: String,
                          url: String)
