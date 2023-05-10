package com.damon.MarketAnalysis

import com.damon.constants.Constants.{AdClickEvent, BlackListWarning, CountByProvince}
import com.damon.utils.common.create_env
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import java.awt.Desktop
import java.net.URL

object AdStatisticsByGeo {

  val blackListOutputTag = new OutputTag[BlackListWarning]("blackList")

  def main(args: Array[String]): Unit = {
    val env = create_env()
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val adEventStream: DataStream[AdClickEvent] = env.readTextFile("C:\\Users\\star\\Desktop\\UserBehaviourAnalysis\\src\\main\\resources\\MarketAnalysis\\AdClickLog.csv")
      .map(data => {
        val dataArrays = data.split(",")
        AdClickEvent(dataArrays(0).trim.toLong, dataArrays(1).trim.toLong, dataArrays(2).trim,
          dataArrays(3).trim, dataArrays(4).trim.toLong)
      })
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[AdClickEvent](Time.seconds(5)) {
        override def extractTimestamp(element: AdClickEvent): Long = element.timestamp * 1000L
      })

    val filterBlackListStream: DataStream[AdClickEvent] = adEventStream
      .keyBy(data => (data.userId, data.adId))
      .process(new FilterBlockProcess(100))

    filterBlackListStream.getSideOutput(blackListOutputTag).print(">>blackList")

    val dataStream: DataStream[CountByProvince] = filterBlackListStream
      .keyBy(_.province)
      .timeWindow(Time.seconds(10), Time.seconds(5))
      .aggregate(new AdCountAgg(), new AdCountResult())

    dataStream.print("ad count")

    env.execute()
  }

  class FilterBlockProcess(maxCount: Int) extends KeyedProcessFunction[(Long, Long), AdClickEvent, AdClickEvent] {

    lazy val countState: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("count_state", classOf[Long]))

    lazy val isSentBlackList: ValueState[Boolean] = getRuntimeContext.getState(new ValueStateDescriptor[Boolean]("is_sent_state", classOf[Boolean]))

    override def processElement(value: AdClickEvent, ctx: KeyedProcessFunction[(Long, Long), AdClickEvent, AdClickEvent]#Context, out: Collector[AdClickEvent]): Unit = {
      val curCount = countState.value()
//      println(">>id: " + value.userId + " curCount: " + curCount)
      // 在数据第一次来的时候注册定时器，当触发定时器的时候，清除状态即可
      if (curCount == 0) {
        // ctx.timerService().currentProcessingTime() 获取当前的处理时间
        //  /(1000 * 60 * 60 * 24)  将毫秒数转换为天数，即计算当前时间距离起始时间点的天数
        // +1 将计算出来的天数+1，表示第二天的零点时刻
        //  *(1000 * 60 * 60 * 24) 将第二天的零点时刻转换为毫秒数，即计算从起始时间点到第二天零点的时间差
        val ts: Long = (ctx.timerService().currentProcessingTime() / (1000 * 60 * 60 * 24) + 1) * (1000 * 60 * 60 * 24)
        ctx.timerService().registerProcessingTimeTimer(ts)
      }

      // 如果点击次数超过了阈值，报警
      if (curCount > maxCount) {
        if (!isSentBlackList.value()) {
          isSentBlackList.update(true)
          ctx.output(blackListOutputTag, BlackListWarning(value.userId, value.adId, "Click over" + maxCount + " times today."))
        }
        return
      }

      countState.update(curCount + 1)
      out.collect(value)

    }

    // timestamp 定时器触发的时间戳，这个时间戳是相对于 flink 处理时间
    // ctx 表示onTimer的上下文对象，该对象用来获取 KeyedProcessFunction 的状态、定时器信息
    // out 表示输出结果的 Collector 对象，可以使用这个对象向下游发送结果数据
    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[(Long, Long), AdClickEvent, AdClickEvent]#OnTimerContext, out: Collector[AdClickEvent]): Unit = {
      isSentBlackList.clear()
      countState.clear()
    }
  }

  class AdCountAgg() extends AggregateFunction[AdClickEvent, Long, Long]{
    override def createAccumulator(): Long = 0L

    override def add(value: AdClickEvent, accumulator: Long): Long = accumulator + 1

    override def getResult(accumulator: Long): Long = accumulator

    override def merge(a: Long, b: Long): Long = a + b
  }

  class AdCountResult() extends WindowFunction[Long, CountByProvince, String, TimeWindow]{
    override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[CountByProvince]): Unit = {
      // input: Iterable[Long] 里面只有一个元素， 所有可以使用 input.iterator.next() 获取到元素
      out.collect(CountByProvince(window.getEnd.toString, key, input.iterator.next()))
    }
  }
}
