package com.damon.HotItemAnalysis

import org.apache.flink.streaming.api.scala._
import com.damon.constants.Constants.{ItemViewCount, UserBehaviour}
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

object HotItems {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val dataSource: DataStream[UserBehaviour] = env.readTextFile("src/main/resources/HotItemAnalysis/UserBehavior.csv")
      .map(data => {
        val dataArray: Array[String] = data.split(",")
        UserBehaviour(
          dataArray(0).trim.toLong,
          dataArray(1).trim.toLong,
          dataArray(2).trim.toInt,
          dataArray(3).trim,
          dataArray(4).trim.toLong
        )
      })
      .assignAscendingTimestamps(_.timestamp * 1000L)

    dataSource
      .filter(_.behaviour == "pv")
      .keyBy(_.itemId)
      .timeWindow(Time.hours(1), Time.minutes(5))
      .aggregate(new CountAgg())

  }
}

// the output of CountAgg is the input of WindowResult
class CountAgg() extends AggregateFunction[UserBehaviour, Long, Long] {
  override def createAccumulator(): Long = 0L

  override def add(in: UserBehaviour, acc: Long): Long = acc + 1

  override def getResult(acc: Long): Long = acc

  override def merge(acc: Long, acc1: Long): Long = acc + acc1
}


class WindowResult extends WindowFunction[Long, ItemViewCount, Long, TimeWindow]{
  override def apply(key: Long, window: TimeWindow, input: Iterable[Long], out: Collector[ItemViewCount]): Unit = {
    out.collect(ItemViewCount(key, window.getEnd, input.iterator.next()))
  }
}