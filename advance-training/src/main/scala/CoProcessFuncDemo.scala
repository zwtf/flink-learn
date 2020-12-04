
import java.text.SimpleDateFormat

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}

import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.co.CoProcessFunction
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * ProcessFunction
 * Flink的底层API,进行复杂的时间处理的自定义接口
 * 特点一:可以通过时间服务进行 "时间穿梭",通过TimeService设置定时器
 * 特点二:副输出,可以在同一个流中操作不同的数据流
 * 注意:定时器只能操作KeyedStream类型的数据,所以对非键值类型数据做操作时,可以通过指定常量Key,但是这样所有数据都为相同key,只有一个分区运行
 *
 *
 *
 * CorPrecessFunction
 * 将两个KeyedStream连接起来操作,常见于数据流和控制流的结合操作
 * 代码功能:
 *  对应规则流中数据只输出时间(10秒和1分钟)
 *
 *
 * */
object CoProcessFuncDemo {

  def main(args: Array[String]): Unit = {

    // set up the streaming execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // use event time for the application
    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)

    import org.apache.flink.api.scala.createTypeInformation
    // switch messages disable filtering of sensor readings for a specific amount of time
    val filterSwitches: DataStream[(String, Long)] = env
      .fromCollection(Seq(
        ("sensor_2", 10 * 1000L), // forward readings of sensor_2 for 10 seconds
        ("sensor_7", 60 * 1000L)) // forward readings of sensor_7 for 1 minute)
      )

    // ingest sensor stream
    val readings: DataStream[SensorReading] = env
      // SensorSource generates random temperature readings
      .addSource(new SensorSource)

    val forwardedReadings = readings
      // connect readings and switches
      .connect(filterSwitches)
      // key by sensor ids
      .keyBy(_.id, _._1)
      // apply filtering CoProcessFunction
      .process(new ReadingFilter)

    forwardedReadings
      .print()

    env.execute("Monitor sensor temperatures.")

  }

}


class ReadingFilter
  //extends CoProcessFunction[SensorReading, (String, Long), SensorReading] {
  extends CoProcessFunction[SensorReading, (String, Long), String] {

  import org.apache.flink.api.scala.createTypeInformation

  // switch to enable forwarding
  lazy val forwardingEnabled: ValueState[Boolean] =
    getRuntimeContext.getState(
      new ValueStateDescriptor[Boolean]("filterSwitch", Types.of[Boolean])
    )

  // hold timestamp of currently active disable timer
  lazy val disableTimer: ValueState[Long] =
    getRuntimeContext.getState(
      new ValueStateDescriptor[Long]("timer", Types.of[Long])
    )

  private val sdf = new SimpleDateFormat("MM-dd_HH:mm:ss")

  override def processElement1(
                                reading: SensorReading,
                                ctx: CoProcessFunction[SensorReading, (String, Long), String]#Context,
                                out: Collector[String]): Unit = {

    // check if we may forward the reading
    if (forwardingEnabled.value()) {

      out.collect(reading+" "+ sdf.format(reading.timestamp))
    }
  }

  override def processElement2(
                                switch: (String, Long),
                                ctx: CoProcessFunction[SensorReading, (String, Long), String]#Context,
                                out: Collector[String]): Unit = {

    // enable reading forwarding
    forwardingEnabled.update(true)
    // set disable forward timer
    val timerTimestamp = ctx.timerService().currentProcessingTime() + switch._2
    val curTimerTimestamp = disableTimer.value()
    if (timerTimestamp > curTimerTimestamp) {
      // remove current timer and register new timer
      ctx.timerService().deleteProcessingTimeTimer(curTimerTimestamp)
      ctx.timerService().registerProcessingTimeTimer(timerTimestamp)
      disableTimer.update(timerTimestamp)
    }
  }

  override def onTimer(
                        ts: Long,
                        ctx: CoProcessFunction[SensorReading, (String, Long), String]#OnTimerContext,
                        out: Collector[String]): Unit = {

    // remove all state. Forward switch will be false by default.
    forwardingEnabled.clear()
    disableTimer.clear()
  }
}