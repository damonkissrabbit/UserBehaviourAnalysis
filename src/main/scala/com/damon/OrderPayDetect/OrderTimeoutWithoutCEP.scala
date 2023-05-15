package com.damon.OrderPayDetect

import com.damon.constants.Constants.{OrderEvent, OrderResult}
import com.damon.utils.common.create_env
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.{TimeCharacteristic, functions}
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector

object OrderTimeoutWithoutCEP {

  val orderTimeoutTag = new OutputTag[OrderResult]("timeout")

  def main(args: Array[String]): Unit = {
    val env = create_env()
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val dataStream: KeyedStream[OrderEvent, Long] = env.readTextFile("C:\\Users\\star\\Desktop\\UserBehaviourAnalysis\\src\\main\\resources\\OrderPayDetect\\OrderLog.csv")
      .map(data => {
        val dataArrays = data.split(",")
        OrderEvent(dataArrays(0).trim.toLong, dataArrays(1).trim, dataArrays(2).trim, dataArrays(3).trim.toLong)
      })
      .assignAscendingTimestamps(_.eventTime * 1000L)
      .keyBy(_.orderId)

    dataStream.process(new OrderTimeoutWarning())

    dataStream.process(new OrderPayMatch())

  }

  class OrderPayMatch() extends KeyedProcessFunction[Long, OrderEvent, OrderResult]{
    override def processElement(value: OrderEvent, ctx: KeyedProcessFunction[Long, OrderEvent, OrderResult]#Context, out: Collector[OrderResult]): Unit = {

    }
  }


  class OrderTimeoutWarning() extends KeyedProcessFunction[Long, OrderEvent, OrderResult]{

    lazy val isPayedState: ValueState[Boolean] = getRuntimeContext.getState(new ValueStateDescriptor[Boolean]("is_payed_state", classOf[Boolean]))

    override def processElement(value: OrderEvent, ctx: KeyedProcessFunction[Long, OrderEvent, OrderResult]#Context, out: Collector[OrderResult]): Unit = {

      // 先取出状态标志位
      val isPayed = isPayedState.value()

      if (value.eventType == "create" && !isPayed){
        // 如果遇到了create时间，并且pay没有来过，注册定时器开始等待
        ctx.timerService().registerEventTimeTimer(value.eventTime * 1000L + 15 * 60 * 1000L)
      } else if (value.eventType == "pay"){
        // 如果是pay事件，直接把状态改为true
        isPayedState.update(true)
      }
    }

    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, OrderEvent, OrderResult]#OnTimerContext, out: Collector[OrderResult]): Unit = {
      // 判断isPayed 是否为true
      val isPayed = isPayedState.value()
      if (isPayed) {
        out.collect(OrderResult(ctx.getCurrentKey, "order payed successfully"))
      } else {
        out.collect(OrderResult(ctx.getCurrentKey, "order timeout"))
      }
      isPayedState.clear()
    }
  }
}
