package com.damon.NetworkFlowAnalysis

import com.damon.constants.Constants.UserBehaviour
import com.damon.utils.common.create_env
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.windowing.time.Time

object PageView {
  def main(args: Array[String]): Unit = {
    val env = create_env()
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val dataStream: DataStream[(String, Int)] = env.readTextFile("C:\\Users\\star\\Desktop\\UserBehaviourAnalysis\\src\\main\\resources\\NetworkFlowAnalysis\\UserBehavior.csv")
      .map(data => {
        val dataArrays = data.split(",")
        UserBehaviour(dataArrays(0).trim.toLong, dataArrays(1).trim.toLong, dataArrays(2).trim.toInt, dataArrays(3).trim, dataArrays(4).trim.toLong)
      }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[UserBehaviour](Time.seconds(5)) {
      override def extractTimestamp(element: UserBehaviour): Long = element.timestamp * 1000L
    })
      .filter(_.behaviour == "pv")
      .map(data => ("pv", 1))
      .keyBy(_._1)
      .timeWindow(Time.seconds(10))
      .sum(1) // 对元组的第二个元素进行求和操作

    dataStream.print("pv count: ")

    env.execute("page view count")
  }
}
