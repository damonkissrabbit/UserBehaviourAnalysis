package com.damon.NetworkFlowAnalysis

import com.damon.constants.Constants.{UserBehaviour, UvCount}
import com.damon.utils.common.create_env
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.shaded.guava18.com.google.common.hash.{BloomFilter, Funnels}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.function.{AllWindowFunction, WindowFunction}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import java.lang

/**
 * 使用BloomFilter来实现uv统计
 * 网站独立访问数 (uv) 统计
 * 关注在一段时间内到底有多少不同的用户访问了网站，流量统计的重要指标是网站的独立访客数（Unique Visitor, UV）
 * UV 指的是一段时间（比如一小时）内访问网站的总人数，1天内同意方可的多次访问只记录为一个访客
 */
object BloomFilterUV {
  def main(args: Array[String]): Unit = {
    val env = create_env()
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val dataStream = env.readTextFile("C:\\Users\\star\\Desktop\\UserBehaviourAnalysis\\src\\main\\resources\\NetworkFlowAnalysis\\UserBehavior.csv")
      .map(data => {
        val dataArrays: Array[String] = data.split(",")
        UserBehaviour(dataArrays(0).trim.toLong, dataArrays(1).trim.toLong, dataArrays(2).trim.toInt, dataArrays(3).trim, dataArrays(4).trim.toLong)
      })
      .assignAscendingTimestamps(_.timestamp * 1000L)
      .filter(_.behaviour == "pv")
      .map(data => ("key", data.userId))
      .keyBy(_._1)
      .timeWindow(Time.minutes(2))
      .aggregate(new BFCountAgg(), new BFWindowResult())

    dataStream.print()
    env.execute()
  }
}

class BFCountAgg() extends AggregateFunction[(String, Long), (Long, BloomFilter[lang.Long]), Long]{
  override def createAccumulator(): (Long, BloomFilter[lang.Long]) = (0, BloomFilter.create(Funnels.longFunnel(), 10000, 0.01))

  override def add(in: (String, Long), accumulator: (Long, BloomFilter[lang.Long])): (Long, BloomFilter[lang.Long]) = {
    if (!accumulator._2.mightContain(in._2)){
      accumulator._2.put(in._2)
      (accumulator._1 + 1, accumulator._2)
    }else {
      accumulator
    }
  }

  override def getResult(accumulator: (Long, BloomFilter[lang.Long])): Long = accumulator._1

  override def merge(a: (Long, BloomFilter[lang.Long]), b: (Long, BloomFilter[lang.Long])): (Long, BloomFilter[lang.Long]) = ???
}

class BFWindowResult() extends WindowFunction[Long, String, String, TimeWindow]{
  override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[String]): Unit = {
    out.collect("window end: " + window.getEnd.toString + ", uv_count: " + input.iterator.next())
  }
}

