package com.damon.LoginFailDetect

import com.damon.constants.Constants.{LoginEvent, Warning}
import com.damon.utils.common.create_env
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

import java.util
import scala.collection.mutable.ListBuffer

object LoginFail {
  def main(args: Array[String]): Unit = {
    val env = create_env()
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val resource = getClass.getResource("/LoginFailDetect/LoginLog.csv")

    val loginEventStream: DataStream[LoginEvent] = env.readTextFile(resource.getPath)
      .map(data => LoginEvent(
        data.split(",")(0).trim.toLong,
        data.split(",")(0).trim,
        data.split(",")(0).trim,
        data.split(",")(0).trim.toLong


      ))
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[LoginEvent](Time.seconds(5)) {
        override def extractTimestamp(element: LoginEvent): Long = element.eventTime * 1000L
      })

    loginEventStream
      .keyBy(_.userId)
      .process(new LoginWarning(2))
  }
}

class LoginWarning(maxFailTimes: Int) extends KeyedProcessFunction[Long, LoginEvent, Warning] {

  lazy val loginFailState: ListState[LoginEvent] = getRuntimeContext.getListState(new ListStateDescriptor[LoginEvent]("login-fail-state", classOf[LoginEvent]))

  override def processElement(value: LoginEvent, ctx: KeyedProcessFunction[Long, LoginEvent, Warning]#Context, out: Collector[Warning]): Unit = {
    val loginFailList = loginFailState.get()

    // 判断理性是否fail，只添加fail的事件到状态
    //    if (value.eventType == "fail") {
    //      if (!loginFailList.iterator().hasNext) { //如果之前没有数据，需要注册定时器
    //        ctx.timerService().registerEventTimeTimer(value.eventTime * 1000L + 2000L)
    //      }
    //      loginFailState.add(value)
    //    } else {
    //      loginFailState.clear()
    //    }

    // 因为如果一下子大量登录失败，其实没必要等到2秒钟后的定时器触发，故可以将定时器进行注释
    if (value.eventType == "fail") {
      // 如果是失败，判断之前是否有登录失败事件
      val iter = loginFailState.get().iterator()
      if (iter.hasNext) {
        // 如果有登录失败事件，就比较事件时间
        val firstFail = iter.next()
        if (value.eventTime < firstFail.eventTime + 2) {
          // 如果两次间隔小于两秒，输出报警
          out.collect(Warning(value.userId, firstFail.eventTime, value.eventTime, "login fail in 2 seconds."))
        }
        // 更新最近一次的登录失败事件，保存在状态里
        loginFailState.clear()
        loginFailState.add(value)
      } else {
        // 如果是第一次登录失败，直接添加到状态
        loginFailState.add(value)
      }
    } else {
      // 如果是成功，清空状态
      loginFailState.clear()
    }

  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, LoginEvent, Warning]#OnTimerContext, out: Collector[Warning]): Unit = {
    val allLoginFails = new ListBuffer[LoginEvent]
    val iter: util.Iterator[LoginEvent] = loginFailState.get.iterator()

    while (iter.hasNext) {
      allLoginFails += iter.next()
    }

    if (allLoginFails.length >= maxFailTimes) {
      out.collect(Warning(allLoginFails.head.userId, allLoginFails.head.eventTime, allLoginFails.last.eventTime, "login fail in 2 seconds for " + allLoginFails.length + " times"))
    }
    loginFailState.clear()
  }

}
