package com.damon.MarketAnalysis

import com.damon.constants.Constants.MarketingViewCount
import com.damon.utils.common.create_env
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import java.sql.Timestamp

object AppMarketingByChannel {
  def main(args: Array[String]): Unit = {
    val env = create_env()
    env.setParallelism(10)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val dataStream: DataStream[MarketingViewCount] = env.addSource(new SimulatedEventSource())
      .assignAscendingTimestamps(_.timestamp)
      .filter(_.behavior != "Uninstall")
      .map(data => ((data.channel, data.behavior), 1L))
      .keyBy(_._1)
      .timeWindow(Time.seconds(10), Time.seconds(5))
      .process(new MarketingCountByChannel())

    dataStream.print()
    env.execute()
  }
}

// window 后面之后使用process的话，使用 ProcessWindowFunction
class MarketingCountByChannel extends ProcessWindowFunction[((String, String), Long), MarketingViewCount, (String, String), TimeWindow]{

  // elements 是全量数据
  // process 方法接受四个参数，分别是输入数据的类型，窗口的类型，键以及context上下文
  // 其中输入数据类型表示输入数据流中的数据类型，窗口类型表示窗口的类型
  // 键用于表示当前窗口的分组键，context则用于提供一些窗口计算过程中的上下文信息
  override def process(key: (String, String), context: Context, elements: Iterable[((String, String), Long)], out: Collector[MarketingViewCount]): Unit = {
    val startTs: String = new Timestamp(context.window.getStart).toString
    val endTs: String = new Timestamp(context.window.getEnd).toString
    val channel: String = key._1
    val behavior: String = key._2
    val count: Int = elements.size
//    println(">>elements: ", elements)
    out.collect(MarketingViewCount(startTs, endTs, channel, behavior, count))
  }
}