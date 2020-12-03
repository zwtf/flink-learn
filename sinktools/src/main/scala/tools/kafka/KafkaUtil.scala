package tools.kafka

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer011, FlinkKafkaProducer011}


object KafkaUtil {

  private val props = new Properties()
  props.setProperty("bootstrap.servers", "hadoop101:9092")
  props.setProperty("group.id", "gmall")
  val brokerList = "192.168.100.101:9092"

  def getConsumer(topic: String): FlinkKafkaConsumer011[String] = {
    val value = new FlinkKafkaConsumer011[String](topic, new SimpleStringSchema(), props)
    value
  }

  def getProducer(topic: String): FlinkKafkaProducer011[String] = {
    val value: FlinkKafkaProducer011[String] = new FlinkKafkaProducer011[String](brokerList, topic, new SimpleStringSchema())
    value
  }


}
