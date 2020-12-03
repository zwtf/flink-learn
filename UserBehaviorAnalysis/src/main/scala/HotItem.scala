import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.java.tuple.{Tuple, Tuple1}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, createTypeInformation}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011
import org.apache.flink.util.Collector
import tools.kafka.{KafkaUtil, MockData}

import scala.collection.mutable.ListBuffer //flink-streaming-scala的包
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment  //flink-streaming-java的包

//输入数据样例类
case class UserBehavior(UserId: Long, itemId: Long, catrgroyId: Int, behavior: String, timestamp: Long)

// 输出数据样例类
case class ItemViewCount(itemId: Long //商品ID
                         , windowEnd: Long //窗口结束时间,表名是单独某窗口
                         , count: Long //该窗口内聚合结果值
                        )

object HotItem {

  def main(args: Array[String]): Unit = {

    val path = "D:\\3_data\\repository\\flink-learn\\UserBehaviorAnalysis\\src\\main\\resources\\UserBehavior.csv"
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1) //方便展示，对结果正确性没有影响
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime) //设置事件时间

    val sourceStream = env.readTextFile(path)
      .map(line => {
        val linearray = line.split(",")

        UserBehavior(linearray(0).toLong,
          linearray(1).toLong,
          linearray(2).toInt,
          linearray(3),
          linearray(4).toLong)
      })
      .filter(_.behavior == "pv")

    //sourceStream.print()

    //指定时间时间使用的字段
    //生产环境一般是乱序的，不指定顺序的事件时间，而是带watermark的乱序
    sourceStream.assignAscendingTimestamps(_.timestamp * 1000)
      .keyBy("itemId")
      .timeWindow(Time.minutes(60), Time.minutes(5))
      .aggregate(new countAggFun(), new WindowResultFun())
      .keyBy("windowEnd")
      .process(new TopNHotItems(3))
      .print()

    env.execute("HotItemAnalysis")

  }

  /**
   * aggregate支持窗口预聚合，不用缓存整个窗口的数据，该函数用于定义预聚合如何操作
   * 泛型含义：IN：传入进行聚合的对象
   * ACC:新对象传入后以什么数据类型参与提前聚合
   * OUT:提前聚合后保留的数据是什么数据类型
   * 注意:预聚合完成后的数据类型，会传入WindowFunction进行整个窗口聚合，两者数据类型需要一致！
   */
  class countAggFun() extends AggregateFunction[UserBehavior, Long, Long] {
    //初始值
    override def createAccumulator(): Long = 0L;

    //具体的聚合逻辑
    override def add(value: UserBehavior, accumulator: Long): Long = accumulator + 1L

    //最后获取结果如何获取
    override def getResult(accumulator: Long): Long = accumulator

    //不同 "子计算节点" 预聚合的数据如何预聚合的
    override def merge(a: Long, b: Long): Long = a + b

  }


  /**
   * 窗口聚合操作，可以脱离预聚合单独存在(需要改下泛型)，也可以结合预聚合的数据做整个窗口聚合
   * 泛型 IN：输入类型，可能是WindowedStream直接传入的数据类型，也可以是预聚合的数据类型
   *     OUT：最终输入类型
   *     Tuple：必须是JavaTuple型，因为KeyBy操作分组的是否就是返回JavaTuple型
   *     TimeWindow：代表操作的窗口，主要就是获取窗口的开始时间、结束时间，也有其他方法
   *
   * key：商品ID，分组的键
   * window：具体的window
   * input：可迭代对象，这里有预聚合，泛型应该是[Long]，而不是[UserBehavior]
   * Collector：负责收集和分发结果,最终输出封装了一下，是[ItemViewCount]，而不是[Long]
   *
   */
  class WindowResultFun extends WindowFunction[Long,ItemViewCount,Tuple,TimeWindow]{
    override def apply(key: Tuple,
                       window: TimeWindow,
                       input: Iterable[Long],
                       out: Collector[ItemViewCount]): Unit = {
      val itemId = key.asInstanceOf[Tuple1[Long]].f0
      val windowEnd = window.getEnd
      val count = input.iterator.next

      out.collect(ItemViewCount(itemId,windowEnd,count))
    }
  }



}
