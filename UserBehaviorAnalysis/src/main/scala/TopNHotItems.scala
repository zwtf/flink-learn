

import java.text.SimpleDateFormat

import java.sql.Timestamp
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

/**
 *
 * K：分组键，这里要统计每个窗口最热门的商品，是按照窗口分组的
 * I:输入类型
 * O:输出类型，这里格式化成String输出
 *
 * 几种State
 * Keyed State
 *    针对 keyed Stream 状态的操作，每个Key 维护一个KeyState
 *    一个算子子任务中处理一个Key的全部数据,状态数据KeyedState就在算子子任务中共享
 *    常用实现：ValueState、MapState、ListState
 *
 * Operator State
 *    整个任务维护一个State,一个算子子任务(即一个节点)维护一个状态
 *    涉及到横向扩/缩容时，有多种实现方式，负责的函数是CheckpointedFunction
 *
 * @param topSize
 */
// 求某个窗口中前 N 名的热门点击商品，key 为窗口时间戳，输出为 TopN 的结果字符串
class TopNHotItems(topSize: Int) extends KeyedProcessFunction[Tuple, ItemViewCount, String] {
  private var itemState:ListState[ItemViewCount] = _

  //每个子计算节点要获取最新状态的获取方法，一般都是根据状态描述符从context中获取
  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    val stateDecriptor = new ListStateDescriptor[ItemViewCount]("itemState-state",classOf[ItemViewCount])
    itemState = getRuntimeContext.getListState(stateDecriptor)
  }

  override def processElement(input: ItemViewCount, context: KeyedProcessFunction[Tuple, ItemViewCount, String]#Context, out: Collector[String]): Unit = {
    // 每条数据都保存到状态中
    itemState.add(input)
    // 注册 windowEnd+1 的 EventTime Timer, 当触发时，说明收齐了属于windowEnd窗口的所有商品数据
    // 也就是当程序看到windowend + 1的水位线watermark时，触发onTimer回调函数
    context.timerService.registerEventTimeTimer(input.windowEnd + 1)
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Tuple, ItemViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {
    // 获取收到的所有商品点击量
    val allItems: ListBuffer[ItemViewCount] = ListBuffer()
    import scala.collection.JavaConversions._
    for (item <- itemState.get) {
      allItems += item
    }
    // 提前清除状态中的数据，释放空间
    itemState.clear()
    // 按照点击量从大到小排序
    val sortedItems = allItems.sortBy(_.count)(Ordering.Long.reverse).take(topSize)
    // 将排名信息格式化成 String, 便于打印
    val result: StringBuilder = new StringBuilder
    result.append("====================================\n")
    result.append("TimeStamp: ").append(timestamp -1 ).append("\n")
    //result.append("TimeStamp: ").append((timestamp -1) * 1000).append("\n")
    result.append("时间: ").append(new Timestamp(timestamp -1) ).append("\n")


    for(i <- sortedItems.indices){
      val currentItem: ItemViewCount = sortedItems(i)
      // e.g.  No1：  商品ID=12224  浏览量=2413
      result.append("No").append(i+1).append(":")
        .append("  商品ID=").append(currentItem.itemId)
        .append("  浏览量=").append(currentItem.count)
        .append("  windowEnd=").append(currentItem.windowEnd).append("\n")

    }
    result.append("====================================\n\n")
    // 控制输出频率，模拟实时滚动结果
    Thread.sleep(1000)
    out.collect(result.toString)
  }
}

