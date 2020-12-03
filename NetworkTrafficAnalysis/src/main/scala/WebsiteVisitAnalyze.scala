

import java.sql.Timestamp
import java.text.SimpleDateFormat

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.api.java.tuple.Tuple2
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.{KeyedProcessFunction, ProcessFunction}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer


case class LogEvent(ipAddr:String, timestamp:Long,method:String,url:String,testField:String)
case class WebsiteViewCount(url:String,windowStart:Long,windowEnd:Long,count: Int)
//case class WebsiteViewCount(url:String,windowStart:String,windowEnd:String,count: Int)

object WebsiteVisitAnalyze{

  def main(args: Array[String]): Unit = {

    val path="D:\\3_data\\repository\\flink-learn\\NetworkTrafficAnalysis\\src\\main\\resources\\apachetest.log"
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

   // import org.apache.flink.api.scala.createTypeInformation //也行
    import org.apache.flink.streaming.api.scala.createTypeInformation
    val sourceStream = env.readTextFile(path)
      .map(line => {
        val sdf = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss")
        val linearray = line.split(" ")
        val timestamp = sdf.parse(linearray(3)).getTime
        LogEvent(linearray(0), timestamp, linearray(5), linearray(6),"1")
      }).filter(_.url != "")

    //sourceStream.print()

    val aggredStream = sourceStream.assignAscendingTimestamps(_.timestamp)
      .keyBy("url", "testField")
      .timeWindow(Time.minutes(10), Time.seconds(5))
      .aggregate(new aggreCountFunc(), new WindowResultFunc())

    //aggredStream.print()

    aggredStream.keyBy("windowStart")
      .process(new WebsiteAnalyzeFunc())
      .print()

    env.execute("WebsiteVisitAnalyze")

  }



}

//预聚合函数
class aggreCountFunc() extends AggregateFunction[LogEvent, Long, Long] {
  //初始值
  override def createAccumulator(): Long = 0L;

  //具体的聚合逻辑
  override def add(value: LogEvent, accumulator: Long): Long = accumulator + 1L

  //最后获取结果如何获取
  override def getResult(accumulator: Long): Long = accumulator

  //不同 "子计算节点" 预聚合的数据如何预聚合的
  override def merge(a: Long, b: Long): Long = a + b

}

//聚合函数
class WindowResultFunc extends WindowFunction[Long,WebsiteViewCount,Tuple,TimeWindow]{
  override def apply(key: Tuple, window: TimeWindow, input: Iterable[Long], collector: Collector[WebsiteViewCount]): Unit = {
    val url = key.asInstanceOf[Tuple2[String, String]].f0
    val startTime: Long = window.getStart
    val endTime: Long = window.getEnd
    val count = input.iterator.next  //已经预聚合过,所以只有一个值

    collector.collect(WebsiteViewCount(url,startTime,endTime,count.toInt))

    //val sdf = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss")
    //val startTimeStr = sdf.format(startTime)
    //val endTimeStr = sdf.format(endTime)
    //collector.collect(WebsiteViewCount(url,startTimeStr,endTimeStr,count.toInt))
  }
}

class WebsiteAnalyzeFunc extends KeyedProcessFunction[Tuple,WebsiteViewCount,String]{

  var itemState:ListState[WebsiteViewCount] = _

  //定义进行状态操作时,子计算节点如何获取历史状态,一般是从运行时上下文中获取
  override def open(parameters: Configuration): Unit = {

    super.open(parameters)
    // getClass 和classOf 是有区别的!!!
    //val stateDescriper = new ListStateDescriptor("urlVisitTopNListState", WebsiteViewCount.getClass)
    val stateDescriper = new ListStateDescriptor("urlVisitTopNListState", classOf[WebsiteViewCount])
    itemState = getRuntimeContext.getListState(stateDescriper)

  }

  //元素处理,定义元素处理逻辑,这里注册了一个触发操作
  override def processElement(value: WebsiteViewCount, ctx: KeyedProcessFunction[Tuple, WebsiteViewCount, String]#Context, out: Collector[String]): Unit = {
    //直接把元素保存到状态信息里面
    itemState.add(value)

    ctx.timerService().registerEventTimeTimer(value.windowEnd + 1) //WindowEnd变化
  }

  //定义触发后的具体操作
  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Tuple, WebsiteViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {

    //从状态中获取全部元素
    val allElem:ListBuffer[WebsiteViewCount] = ListBuffer()
    import scala.collection.JavaConversions._
    for (elem <- itemState.get){
      allElem += elem
    }
    //清理状态数据,节省空间.处理完一波状态信息就需要清理
    itemState.clear()

    //排序
    allElem.sortBy(_.count)(Ordering.Int.reverse)

    //结果格式化
    var result: StringBuilder = new StringBuilder
    result.append("====================================\n")
    result.append("时间: ").append(new Timestamp(timestamp - 1)).append("\n")

    for (i <- allElem.indices) {
      val currentUrlView: WebsiteViewCount = allElem(i)
      // e.g.  No1：  URL=/blog/tags/firefox?flav=rss20  流量=55
      result.append("No").append(i+1).append(":")
        .append("  URL=").append(currentUrlView.url)
        .append("  流量=").append(currentUrlView.count).append("\n")
    }
    result.append("====================================\n\n")
    // 控制输出频率，模拟实时滚动结果
    Thread.sleep(1000)
    out.collect(result.toString)

  }
}