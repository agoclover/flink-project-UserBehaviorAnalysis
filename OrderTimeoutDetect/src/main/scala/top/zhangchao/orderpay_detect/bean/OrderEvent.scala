package top.zhangchao.orderpay_detect.bean

/**
 * <p>Title: </p>
 *
 * <p>Description: </p>
 *
 * @author Zhang Chao
 * @version java_day
 * @date 2020/9/9 9:01 上午
 */
case class OrderEvent(orderId: Long,
                      eventType: String,
                      txId:String,
                     timestamp: Long)
