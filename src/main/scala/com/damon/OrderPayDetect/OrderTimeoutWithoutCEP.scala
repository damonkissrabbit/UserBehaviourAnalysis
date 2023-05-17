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

//    dataStream.process(new OrderTimeoutWarning())

    val orderResultStream: DataStream[OrderResult] = dataStream.process(new OrderPayMatch())

    orderResultStream.print("payed")
    orderResultStream.getSideOutput(orderTimeoutTag).print("timeout")

    env.execute()

  }

  class OrderPayMatch() extends KeyedProcessFunction[Long, OrderEvent, OrderResult]{

    lazy val isPayedState: ValueState[Boolean] = getRuntimeContext.getState(new ValueStateDescriptor[Boolean]("isPayedState", classOf[Boolean]))

    lazy val timerState: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("timer_state", classOf[Long]))

    override def processElement(value: OrderEvent, ctx: KeyedProcessFunction[Long, OrderEvent, OrderResult]#Context, out: Collector[OrderResult]): Unit = {

      val isPayed: Boolean = isPayedState.value()
      val timerTs: Long = timerState.value()

      if (value.eventType == "create"){
        if (isPayed){
          // 这里就是 pay 数据先到了，然后 create 数据才到
          // 直接将数据发送出去就行，然后清空state状态
          out.collect(OrderResult(value.orderId, "payed successfully"))
          ctx.timerService().deleteEventTimeTimer(timerTs)
          isPayedState.clear()
          timerState.clear()
        } else {
          // create 数据先到，然后注册一个 15 分钟的定时器
          //为什么要注册定时器，因为可能只有 create 数据，而没有 pay 数据
          val ts: Long = value.eventTime * 1000L + 15 * 60 * 1000L
          ctx.timerService().registerEventTimeTimer(ts)
          timerState.update(ts)
        }
      } else if (value.eventType == "pay"){
        // 如果 pay 数据先到了
        if (timerTs > 0){
          // 未超时
          if (timerTs > value.eventTime * 1000L){
            out.collect(OrderResult(value.orderId, "payed successfully"))
          } else {
            // 超时了
            ctx.output(orderTimeoutTag, OrderResult(value.orderId, "payed but already timeout"))
          }
          // 清空状态即可
          ctx.timerService().deleteEventTimeTimer(timerTs)
          isPayedState.clear()
          timerState.clear()
        } else {
          // pay 数据先到，更新isPayedState状态，注册一个当前时间的定时器
          isPayedState.update(true)
          ctx.timerService().registerEventTimeTimer(value.eventTime * 1000L)
          timerState.update(value.eventTime * 1000L)
        }
      }
    }

    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, OrderEvent, OrderResult]#OnTimerContext, out: Collector[OrderResult]): Unit = {
      // 触发定时器的情况
     if (isPayedState.value()) {
       // 已经支付过了，但是没有create 数据
       ctx.output(orderTimeoutTag, OrderResult(ctx.getCurrentKey, "already payed but not found create log"))
     } else {
       // 注册时间超时了
       ctx.output(orderTimeoutTag, OrderResult(ctx.getCurrentKey, "order timeout"))
     }
    }
  }


  // valueState 初始化值为 false
  // 代码逻辑，同一个orderId会进入这里，如果是create事件，那么注册一个15分钟之后的定时器
  // 如果是pay事件，那么就把 valueState 里面的状态改为true，当定时器触发的时候，就根据 valueState 里面的状态来判断是否已经支付过
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
