package com.damon.NetworkFlowAnalysis

import com.damon.constants.Constants.{UserBehaviour, UvCount}
import com.damon.utils.common.create_env
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.function.AllWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
 * 网站独立访问数 (uv) 统计
 * 关注在一段时间内到底有多少不同的用户访问了网站，流量统计的重要指标是网站的独立访客数（Unique Visitor, UV）
 * UV 指的是一段时间（比如一小时）内访问网站的总人数，1天内同意方可的多次访问只记录为一个访客
 */
object UniqueVisitor {
  def main(args: Array[String]): Unit = {
    val env = create_env()
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val dataStream: DataStream[UvCount] = env.readTextFile("C:\\Users\\star\\Desktop\\UserBehaviourAnalysis\\src\\main\\resources\\NetworkFlowAnalysis\\UserBehavior.csv")
      .map(data => {
        val dataArrays: Array[String] = data.split(",")
        UserBehaviour(dataArrays(0).trim.toLong, dataArrays(1).trim.toLong, dataArrays(2).trim.toInt, dataArrays(3).trim, dataArrays(4).trim.toLong)
      })
      .assignAscendingTimestamps(_.timestamp * 1000L)
      .filter(_.behaviour == "pv")
      .timeWindowAll(Time.seconds(10))
      .apply(new UvCountByWindow())

    dataStream.print()
    env.execute()
  }
}

class UvCountByWindow() extends AllWindowFunction[UserBehaviour, UvCount, TimeWindow] {
  override def apply(window: TimeWindow, input: Iterable[UserBehaviour], out: Collector[UvCount]): Unit = {
    var idSet: Set[Long] = Set[Long]()
    for (userBehavior <- input) {
      idSet += userBehavior.userId
    }
    out.collect(UvCount(window.getEnd, idSet.size))
  }
}
