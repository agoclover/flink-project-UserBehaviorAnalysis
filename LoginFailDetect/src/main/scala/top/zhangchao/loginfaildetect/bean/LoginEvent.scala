package top.zhangchao.loginfaildetect.bean

/**
 * <p>Title: </p>
 *
 * <p>Description: </p>
 *
 * @author Zhang Chao
 * @version java_day
 * @date 2020/9/8 11:49 上午
 */
case class  LoginEvent(userId: Long,
                       ip: String,
                       eventType: String,
                       eventTime: Long)
