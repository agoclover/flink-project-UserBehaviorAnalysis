package top.zhangchao.loginfaildetect.bean

/**
 * <p>Title: </p>
 *
 * <p>Description: </p>
 *
 * @author Zhang Chao
 * @version java_day
 * @date 2020/9/8 11:50 上午
 */
case class LoginFailWarning(userId: Long,
                            firstFailTime: Long,
                            lastFailTime: Long,
                            msg: String)
