package top.zhangchao.source

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import scala.io.BufferedSource

/**
 * <p>Title: </p>
 *
 * <p>Description: </p>
 *
 * @author Zhang Chao
 * @version java_day
 * @date 2020/9/5 4:18 下午
 */
object KafkaProducerUtil {

  def writeToKafka(topic: String): Unit = {
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "hadoop102:9092")
    properties.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    properties.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    val producer = new KafkaProducer[String, String](properties)
    val path = "/Users/amos/Learning/UserBehaviorAnalysis/HotItemsAnalysis/src/main/resources/UserBehavior.csv"
    val bufferedSource: BufferedSource = io.Source.fromFile(path)

    for(line <- bufferedSource.getLines()){
      val record = new ProducerRecord[String, String](topic, line)
      producer.send(record)
    }
  }

  def main(args: Array[String]): Unit = {
    writeToKafka("hotitems")
  }
}
