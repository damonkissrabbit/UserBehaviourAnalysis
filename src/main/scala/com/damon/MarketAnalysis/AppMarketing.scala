package com.damon.MarketAnalysis

import com.damon.constants.Constants.MarketingViewCount
import com.damon.utils.common.create_env
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import java.sql.Timestamp

object AppMarketing {
  def main(args: Array[String]): Unit = {
    val env = create_env()
    env.setParallelism(2)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val dataStream: DataStream[MarketingViewCount] = env.addSource(new SimulatedEventSource)
      .assignAscendingTimestamps(_.timestamp)
      .filter(_.behavior != "Uninstall")
      .map(_ => ("dummyKey", 1L))
      .keyBy(_._1)
      .timeWindow(Time.seconds(10), Time.seconds(5))
      .aggregate(new CountAgg(), new MarketingCountTotal())

    dataStream.print()

    env.execute()
  }
}
// countAgg[In, Acc, Out] 的 out 就是 MarketingCountTotal[In, Out, Key, Window] 的 in
class CountAgg extends AggregateFunction[(String, Long), Long, Long]{
  override def createAccumulator(): Long = 0L

  override def add(value: (String, Long), accumulator: Long): Long = {
//    println(">>>accumulator: ", accumulator)
    accumulator + 1
  }

  override def getResult(accumulator: Long): Long = accumulator

  override def merge(a: Long, b: Long): Long = a + b
}

class MarketingCountTotal extends WindowFunction[Long, MarketingViewCount, String, TimeWindow]{
  override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[MarketingViewCount]): Unit = {
    val startTs = new Timestamp(window.getStart).toString
    val endTs = new Timestamp(window.getEnd).toString
    val count = input.iterator.next()
//    println(">>>key: ", key)
//    println(">>>input: ", input)
//    println(">>>count: ", count)
    out.collect(MarketingViewCount(startTs, endTs, "app marketing", "total", count))
  }
}
