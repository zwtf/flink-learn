package com.flink.learn.wc

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

object StraeamWcApp {

  def main(args: Array[String]): Unit = {

    //从外部命令获取参数
    val tool = ParameterTool.fromArgs(args)
    val host: String = tool.get("host")
    val port: Int = tool.get("port").toInt

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val textStream: DataStream[String] = env.socketTextStream(host, port)

    import org.apache.flink.api.scala.createTypeInformation
    val dStream = textStream
      .flatMap(_.split(" "))
      .filter(_.nonEmpty)
      .map((_, 1))
      .keyBy(0)
      .sum(1)

    dStream.print()

    env.execute()


  }


}
