
import org.apache.flink.cep.scala.CEP
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{OutputTag, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time

case class OrderEvent(orderId: Long, eventType: String, eventTime: Long)

case class OrderResult(orderId: Long, eventType: String)

/** 订单超时检测
 * 检测下单(create) 和 付款(pay) 两个操作之间的间隔时间,超出15分钟的报警
 *
 * 用到了Flink的侧输出功能
 *
 * 对一个流操作的同时,输出两个结果流:
 *
 *
 */
object OrderTimeout {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    import org.apache.flink.streaming.api.scala.createTypeInformation
    val orderEventStream = env.fromCollection(List(
      OrderEvent(1, "create", 1558430842),
      OrderEvent(2, "create", 1558430843),
      OrderEvent(2, "pay", 1558430844)
    )).assignAscendingTimestamps(_.eventTime * 1000)

    // 定义一个带匹配时间窗口的模式
    val orderPayPattern = Pattern.begin[OrderEvent]("begin")
      .where(_.eventType == "create")
      .next("next")
      .where(_.eventType == "pay")
      .within(Time.minutes(15))

    // 定义一个侧输出标签
    val orderTimeoutOutput = OutputTag[OrderResult]("ordertime")

    // 订单事件流根据 orderId 分流，然后在每一条流中匹配出定义好的模式
    val patternStream = CEP.pattern(orderEventStream.keyBy("orderId"), orderPayPattern)

    /**
     * patternTimeoutFunction 对不满足Pattern(超时)的数据执行这个流,该流返回类型受定义的OutPutTag控制
     * patternSelectFunction  对满足Pattern的数据执行该操作
     * 对
     */
    //混合流,包含符合条件的结果和侧输出结果的流,但是直接print 只会输出PatternSelectFunction处理的数据
    val mixtureStream = patternStream.select(orderTimeoutOutput) {
      // 对于已超时的部分模式匹配的事件序列，会调用这个函数
      (pattern: collection.Map[String, Iterable[OrderEvent]], timestamp: Long) => {
        val createOrder = pattern.get("begin")
        OrderResult(createOrder.get.iterator.next().orderId, "timeout")
      }
    } { // 检测到定义好的模式序列时，就会调用这个函数
      pattern: collection.Map[String, Iterable[OrderEvent]] => {
        val payOrder = pattern.get("next")
        OrderResult(payOrder.get.iterator.next().orderId, "success")
      }
    }

    // 拿到同一输出标签中的 timeout 匹配结果（流）
    val timeoutResult = mixtureStream.getSideOutput(orderTimeoutOutput)

    mixtureStream.print()
    timeoutResult.print()

    env.execute("Order Timeout Detect Job")

  }

}
