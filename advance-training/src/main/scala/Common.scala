import java.util.Calendar

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.util.Random

class Common {

}


case class SensorReading(id: String, timestamp: Long, temperature: Double)
//case class SensorReading(id: String, timeStr: String, temperature: Double)

//书里面Copy下来的.数据源Mock函数
class SensorSource extends RichParallelSourceFunction[SensorReading] {

  // flag indicating whether source is still running.
  var running: Boolean = true

  /** run() continuously emits SensorReadings by emitting them through the SourceContext. */
  override def run(srcCtx: SourceContext[SensorReading]): Unit = {

    // initialize random number generator
    val rand = new Random()
    // look up index of this parallel task
    val taskIdx = this.getRuntimeContext.getIndexOfThisSubtask

    // initialize sensor ids and temperatures
    var curFTemp = (1 to 10).map {
      i => ("sensor_" + (taskIdx * 10 + i), 65 + (rand.nextGaussian() * 20))
    }

    // emit data until being canceled
    while (running) {

      // update temperature
      curFTemp = curFTemp.map( t => (t._1, t._2 + (rand.nextGaussian() * 0.5)) )
      // get current time
      val curTime = Calendar.getInstance.getTimeInMillis

      // emit new SensorReading
      curFTemp.foreach( t => srcCtx.collect(SensorReading(t._1, curTime, t._2)))

      // wait for 100 ms
      Thread.sleep(100)
    }

  }

  /** Cancels this SourceFunction. */
  override def cancel(): Unit = {
    running = false
  }

}

//处理乱序时间 : watermark
class SensorTimeAssigner
  extends BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.seconds(5)) {

  /** Extracts timestamp from SensorReading. */
  override def extractTimestamp(r: SensorReading): Long = r.timestamp

}




object AverageSensorReadings {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.getConfig.setAutoWatermarkInterval(1000L)

    import org.apache.flink.api.scala.createTypeInformation
    val sensorData: DataStream[SensorReading] = env
      .addSource(new SensorSource)
      .assignTimestampsAndWatermarks(new SensorTimeAssigner)

    val avgTemp: DataStream[SensorReading] = sensorData
      // convert Fahrenheit to Celsius using an inlined map function
      .map( r =>
        SensorReading(r.id, r.timestamp, (r.temperature - 32) * (5.0 / 9.0)) )
      // organize stream by sensorId
      .keyBy(_.id)
      // group readings in 1 second windows
      .timeWindow(Time.seconds(1))
      // compute average temperature using a user-defined function
      .apply(new TemperatureAverager)

    // print result stream to standard out
    avgTemp.print()

    // execute application
    env.execute("Compute average sensor temperature")
  }

}



/** User-defined WindowFunction to compute the average temperature of SensorReadings */
class TemperatureAverager extends WindowFunction[SensorReading, SensorReading, String, TimeWindow] {

  /** apply() is invoked once for each window */
  override def apply(
                      sensorId: String,
                      window: TimeWindow,
                      vals: Iterable[SensorReading],
                      out: Collector[SensorReading]): Unit = {

    // compute the average temperature
    val (cnt, sum) = vals.foldLeft((0, 0.0))((c, r) => (c._1 + 1, c._2 + r.temperature))
    val avgTemp = sum / cnt

    // emit a SensorReading with the average temperature
    out.collect(SensorReading(sensorId, window.getEnd, avgTemp))
  }
}