package com.damon.LoginFailDetect

import com.damon.constants.Constants.{LoginEvent, Warning}
import com.damon.utils.common.create_env
import org.apache.flink.cep.PatternSelectFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.cep.scala.CEP

import java.util

object LoginFailWithCEP {
  def main(args: Array[String]): Unit = {
    val env = create_env()
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val resource = getClass.getResource("/LoginFailDetect/LoginLog.csv")

    val loginEventStream: DataStream[LoginEvent] = env.readTextFile(resource.getPath)
      .map(data => {
        val dataArray = data.split(",")
        LoginEvent(
          dataArray(0).trim.toLong, dataArray(1).trim, dataArray(2).trim, dataArray(3).trim.toLong
        )
      }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[LoginEvent](Time.seconds(5)) {
      override def extractTimestamp(element: LoginEvent): Long = element.eventTime * 1000L
    })
      .keyBy(_.userId)


    // 使用CEP方式，先定义匹配规则Pattern，然后在 CEP.pattern 中将数据流和匹配规则放进去得到 patternStream
    // 最后在 patternStream 上使用 select 方法得到输出流
    val loginFailPattern = Pattern
      .begin[LoginEvent]("begin")
      .where(_.eventType == "fail")
      .next("next")
      .where(_.eventType == "fail")
      .within(Time.seconds(2))

    val patternStream = CEP.pattern(loginEventStream, loginFailPattern)

    val loginFailDataStream = patternStream.select(new LoginFailMatch())

    loginFailDataStream.print()

    env.execute()
  }
}

class LoginFailMatch() extends PatternSelectFunction[LoginEvent, Warning] {
  override def select(map: util.Map[String, util.List[LoginEvent]]): Warning = {
    // map.get("begin") 获取到的是一个 util.List， List.iterator.next() 就是获取到第一个元素
    val firstFail = map.get("begin").iterator().next()
    val lastFail = map.get("next").iterator().next() // 这里的 List.iterator.next() 获取到的是第二个元素
    Warning(firstFail.userId, firstFail.eventTime, lastFail.eventTime, "login fail!")
  }
}
