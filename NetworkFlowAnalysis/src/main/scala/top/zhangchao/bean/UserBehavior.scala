package top.zhangchao.bean

/**
 * <p>Title: </p>
 *
 * <p>Description: </p>
 *
 * @author Zhang Chao
 * @version java_day
 * @date 2020/9/5 1:58 下午
 */
case class UserBehavior(userId: Long,
                        itemId: Long,
                        categoryId: Int,
                        behavior: String,
                        timestamp: Long)
