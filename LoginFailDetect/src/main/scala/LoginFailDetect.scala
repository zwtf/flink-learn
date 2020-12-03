import org.apache.flink.cep.scala.CEP
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time


/**
 * 恶意登录检测:
 * 连续两秒内存在登录失败情况的检测,IP可以不同也可以相同
 *
 * 有两种实现方式:
 * 1.基于状态编程:实时事件时间聚合,统计2秒的时间窗口内,登录失败次数超过2次的并输出
 * 缺点:代码量大,且对于逻辑复杂,事件复杂的情况不适用
 * 2.CEP编程 : 模式匹配,多个简单事件链式
 */

case class LoginEvent(userId: Long, ip: String, eventType: String, eventTime: Long)

object LoginFailDetect {
  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    import org.apache.flink.streaming.api.scala.createTypeInformation
    val loginEventStream = env.fromCollection(List(
      LoginEvent(1, "192.168.0.1", "fail", 1558430842),
      LoginEvent(1, "192.168.0.2", "fail", 1558430843),
      LoginEvent(1, "192.168.0.3", "fail", 1558430844),
      LoginEvent(2, "192.168.10.10", "success", 1558430845)
    )).assignAscendingTimestamps(_.eventTime * 1000)

    //定义匹配模式
    val loginFailPattern = Pattern
      .begin[LoginEvent]("begin")
      .where(_.eventType == "fail")
      .next("next")
      .where(_.eventType == "fail")
      .within(Time.seconds(2))

    // 在数据流中匹配出定义好的模式
    val patternStream = CEP.pattern(loginEventStream.keyBy(_.userId), loginFailPattern)


    val loginFailDataStream = patternStream
      .select((pattern: scala.collection.Map[String, Iterable[LoginEvent]]) => {
        //Map包引错了
        //.select((pattern: Map[String, Iterable[LoginEvent]]) => {
        val first = pattern.getOrElse("begin", null).iterator.next()
        val second = pattern.getOrElse("next", null).iterator.next()
        (first.ip, second.ip, second.eventType)
      })

    // 将匹配到的符合条件的事件打印出来
    loginFailDataStream.print()
    env.execute("Login Fail Detect Job")

  }

}
