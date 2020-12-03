package tools.kafka

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer011, FlinkKafkaProducer011}

object StartupApp {

  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val kafkaConsumer: FlinkKafkaConsumer011[String] = KafkaUtil.getConsumer("TestTopic")

    import org.apache.flink.api.scala.createTypeInformation
    val dStream: DataStream[String] = env.addSource(kafkaConsumer)

    dStream.print()

    env.execute()
  }

}
