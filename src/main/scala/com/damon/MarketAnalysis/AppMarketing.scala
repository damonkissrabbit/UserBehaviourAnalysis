package com.damon.MarketAnalysis

import com.damon.utils.common.create_env
import org.apache.flink.streaming.api.TimeCharacteristic

object AppMarketing {
  def main(args: Array[String]): Unit = {
    val env = create_env()
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)


  }
}
