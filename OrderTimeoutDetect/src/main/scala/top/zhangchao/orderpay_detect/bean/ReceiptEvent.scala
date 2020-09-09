package top.zhangchao.orderpay_detect.bean

/**
 * <p>Title: </p>
 *
 * <p>Description: </p>
 *
 * @author Zhang Chao
 * @version java_day
 * @date 2020/9/9 2:27 下午
 */
case class ReceiptEvent(txId: String,
                        payChannel: String,
                        timestamp: Long)
