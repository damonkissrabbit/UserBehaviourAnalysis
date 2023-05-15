package com.damon.OrderPayDetect

import com.damon.constants.Constants.{OrderEvent, OrderResult}
import com.damon.utils.common.create_env
import org.apache.flink.cep.{PatternSelectFunction, PatternTimeoutFunction}
import org.apache.flink.cep.scala.{CEP, PatternStream}
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.windowing.time.Time

import java.util


/**
 * 用户下单后，应设置订单失效时间，以提高用户支付的意愿，并降低系统风险
 * 用户下单扣15分钟未支付，则输出监控信息
 *
 * 利用CEP进行时间流的模式匹配，并设定匹配的时间间隔
 * 也可以利用状态编程，用process function实现处理逻辑
 *
 * 订单支付实时监控，CEP实现
 */

object OrderTimeout {
  def main(args: Array[String]): Unit = {
    val env = create_env()
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val orderEventStream: KeyedStream[OrderEvent, Long] = env.readTextFile("C:\\Users\\star\\Desktop\\UserBehaviourAnalysis\\src\\main\\resources\\OrderPayDetect\\OrderLog.csv")
      .map(data => {
        val dataArrays = data.split(",")
        OrderEvent(dataArrays(0).trim.toLong, dataArrays(1).trim, dataArrays(2).trim, dataArrays(3).trim.toLong)
      })
      .assignAscendingTimestamps(_.eventTime * 1000L)
      .keyBy(_.orderId)

    val orderPayPattern: Pattern[OrderEvent, OrderEvent] = Pattern.begin[OrderEvent]("begin")
      .where(_.eventType == "create")
      .followedBy("follow").where(_.eventType == "pay")
      .within(Time.minutes(15))

    val patternStream: PatternStream[OrderEvent] = CEP.pattern(orderEventStream, orderPayPattern)

    val orderTimeOutputTag = new OutputTag[OrderResult]("orderTimeout")

    val resultStream = patternStream.select(orderTimeOutputTag, new OrderTimeoutSelect(), new OrderPaySelect())

    resultStream.print("payed")
    resultStream.getSideOutput(orderTimeOutputTag).print("timeout")

    env.execute("order time job")
  }
}

class OrderTimeoutSelect() extends PatternTimeoutFunction[OrderEvent, OrderResult]{
  override def timeout(map: util.Map[String, util.List[OrderEvent]], l: Long): OrderResult = {
    val timeoutOrderId = map.get("begin").iterator().next().orderId
    OrderResult(timeoutOrderId, "timeout")
  }
}

class OrderPaySelect() extends PatternSelectFunction[OrderEvent, OrderResult]{
  override def select(map: util.Map[String, util.List[OrderEvent]]): OrderResult = {
    val payOrderId = map.get("follow").iterator().next().orderId
    OrderResult(payOrderId, "payed successfully")
  }
}