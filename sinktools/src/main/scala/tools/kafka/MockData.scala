package tools.kafka

import org.apache.flink.streaming.api.datastream.DataStreamSink
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011

import scala.util.Random


object MockData {


  def main(args: Array[String]): Unit = {

    val topic = "TestTopic"
    val filePath = "D:\\6_tmp\\wc.txt"
    //    val filePath = "D:\\6_tmp\\wc2.txt"

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(1)

    val dStream: DataStream[String] = env.readTextFile(filePath)
    val kafkaProducer: FlinkKafkaProducer011[String] = KafkaUtil.getProducer(topic)

    import org.apache.flink.api.scala.createTypeInformation
    val resStream = dStream
      .flatMap(_.split("\r\n"))
      .filter(!_.matches("^ *$"))
      .map(line => {
        Thread.sleep(new Random().nextInt(2000)); //随机延时发送模拟实际生产效果
        line
      })

    resStream
      .addSink(kafkaProducer)

    env.execute()





    /*
       *构建的测试代码
        val lineStream = dStream.flatMap(_.split("\r\n"))

        //排除掉空行
        val filterStream = lineStream.filter(!_.matches("^ *$"))

        //filterStream.print("filterStream:")

        val data2 = filterStream
          .map(line => {
            Thread.sleep(new scala.util.Random().nextInt(1000))
            val wordsArray = line.split(" ")
            //println("First Word : " + wordsArray(0) + " ,line words: " + wordsArray.length)
            "First Word : " + wordsArray(0) + " ,line words: " + wordsArray.length
          })

        data2
          .addSink(kafkaProducer)

        env.execute()
        */


  }


  def getMockData(env: StreamExecutionEnvironment): Unit ={

    val topic = "TestTopic"
    val filePath = "D:\\6_tmp\\wc.txt"
    //    val filePath = "D:\\6_tmp\\wc2.txt"


    val dStream: DataStream[String] = env.readTextFile(filePath)
    val kafkaProducer: FlinkKafkaProducer011[String] = KafkaUtil.getProducer(topic)

    import org.apache.flink.api.scala.createTypeInformation
    val resStream = dStream
      .flatMap(_.split("\r\n"))
      .filter(!_.matches("^ *$"))
      .map(line => {
        Thread.sleep(new Random().nextInt(1000)); //随机延时发送模拟实际生产效果
        line
      })

    resStream
      .addSink(kafkaProducer)

    //env.execute()



  }




}
