
import org.apache.flink.api.common.state.{MapState, MapStateDescriptor}
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.{KeyedProcessFunction, ProcessFunction}
import org.apache.flink.streaming.api.scala.{OutputTag, StreamExecutionEnvironment}
import org.apache.flink.util.Collector

/**
 * 只输出 长度<=5 的 word count,同时输出 长度>5 的word有哪些
 * */

object WordCountWithSideOutput {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val inputPath = "D:\\6_tmp\\wc.txt"
    import org.apache.flink.streaming.api.scala.createTypeInformation
    val sourceStream = env
      .readTextFile(inputPath)
      .filter(line => !line.matches("^$") && !line.matches("^ *$")) //排除空行和全空格行
      .flatMap(_.split(" "))
      .map((_,1))
      .keyBy(0)

    val outputTag = OutputTag[String]("rejectedWord")

    val complexStream = sourceStream.process(new KeyedProcessFunction[Tuple,(String,Int),(String,Int)] {

      var acceptMap: MapState[String, Int] = _

      override def open(parameters: Configuration): Unit = {
        super.open(parameters)
        val countStateDescriptor = new MapStateDescriptor[String, Int]("countState", classOf[String], classOf[Int])
        acceptMap = getRuntimeContext.getMapState(countStateDescriptor)
      }

      override def processElement(value: (String, Int), ctx: KeyedProcessFunction[Tuple, (String, Int), (String, Int)]#Context, out: Collector[(String, Int)]): Unit = {
       val input = value._1
        if (input.length <= 5) {
          val count = acceptMap.get(input)
          if (count != null) acceptMap.put(input, count + 1) else acceptMap.put(input, 1)
          out.collect((input, acceptMap.get(input)))
        } else
          ctx.output(outputTag, input)
      }
    })

    val sideStream = complexStream.getSideOutput(outputTag)

    sideStream.print("reject word: ")
    complexStream.print()

    env.execute("WordCountWithSideOutput")

  }


}



/*
new KeyedProcessFunction[String, (String, Int)] {





  override def processElement(value: String, ctx: ProcessFunction[String, (String, Int)]#Context, out: Collector[(String, Int)]): Unit = {
    if (input.length <= 5) {
      val count = acceptMap.get(input)
      if (count != null) acceptMap.put(input, count + 1) else acceptMap.put(input, 1)
      out.collect((input, acceptMap.get(input)))
    } else
      ctx.output(outputTag, input)
  }

}*/



/*
new KeyedProcessFunction[Tuple, String, (String, Int)] {
  var acceptMap: MapState[String, Int] = _

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    val countStateDescriptor = new MapStateDescriptor[String, Int]("countState", classOf[String], classOf[Int])
    acceptMap = getRuntimeContext.getMapState(countStateDescriptor)
  }

  override def processElement(input: String, ctx: KeyedProcessFunction[Tuple, String, (String, Int)]#Context, out: Collector[(String, Int)]): Unit = {
    if (input.length <= 5) {
      val count = acceptMap.get(input)
      if (count != null) acceptMap.put(input, count + 1) else acceptMap.put(input, 1)
      out.collect((input, acceptMap.get(input)))
    } else
      ctx.output(outputTag, input)

  }
}*/
