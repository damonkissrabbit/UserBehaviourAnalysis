package com.damon.OrderPayDetect

import com.damon.constants.Constants.{OrderEvent, ReceiptEvent}
import com.damon.utils.common.create_env
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.co.CoProcessFunction
import org.apache.flink.util.Collector

/***
 * 用户下单支付后，应该查询到账信息，进行实施对账
 * 如果有不铺配的支付信息或者到账信息，输出提示信息
 *
 * 从两条刘中分别读取订单支付信息和到账信息，合并处理
 * 用connect连接合并两条流，用coProcessFunction做匹配处理
 *
 */
object TxMatchDetect {

  val unmatchedPays = new OutputTag[OrderEvent]("unmatchedPays")
  val unmatchedReceipts = new OutputTag[ReceiptEvent]("unmatchedReceipts")

  def main(args: Array[String]): Unit = {
    val env = create_env()
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val orderEventStream: KeyedStream[OrderEvent, String] = env.socketTextStream("localhost", 7777)
      .map(data => {
        val dataArrays = data.split(",")
        OrderEvent(dataArrays(0).trim.toLong, dataArrays(1).trim, dataArrays(2).trim, dataArrays(3).trim.toLong)
      })
      .filter(_.txId != "")
      .assignAscendingTimestamps(_.eventTime * 1000L)
      .keyBy(_.txId)

    val receiptEventStream = env.socketTextStream("localhost", 8888)
      .map(data => {
        val dataArray: Array[String] = data.split(",")
        ReceiptEvent(dataArray(0).trim, dataArray(1).trim, dataArray(2).trim.toLong)
      })
      .assignAscendingTimestamps(_.eventTime * 1000L)
      .keyBy(_.txId)

    val processedStream: DataStream[(OrderEvent, ReceiptEvent)] = orderEventStream.connect(receiptEventStream)
      .process(new TxPayMatch())

    processedStream.print("matched")
    processedStream.getSideOutput(unmatchedPays).print("unmatchedPay")
    processedStream.getSideOutput(unmatchedReceipts).print("unmatchedReceipts")

    env.execute()
  }

  class TxPayMatch() extends CoProcessFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]{

    lazy val payState: ValueState[OrderEvent] = getRuntimeContext.getState(new ValueStateDescriptor[OrderEvent]("pay_state", classOf[OrderEvent]))
    lazy val receiptState: ValueState[ReceiptEvent] = getRuntimeContext.getState(new ValueStateDescriptor[ReceiptEvent]("receipt_state", classOf[ReceiptEvent]))

    override def processElement1(pay: OrderEvent, ctx: CoProcessFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]#Context, out: Collector[(OrderEvent, ReceiptEvent)]): Unit = {
      val receipt = receiptState.value()
      if (receipt != null){
        // 如果已经有receipt， 在主流输出匹配信息
        out.collect((pay, receipt))
        receiptState.clear()
      } else {
        payState.update(pay)
        ctx.timerService().registerEventTimeTimer(pay.eventTime * 1000L + 5000L)
      }
    }

    override def processElement2(receipt: ReceiptEvent, ctx: CoProcessFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]#Context, out: Collector[(OrderEvent, ReceiptEvent)]): Unit = {
      val pay = payState.value()
      if(pay != null){
        out.collect(pay, receipt)
        payState.clear()
      } else {
        receiptState.update(receipt)
        ctx.timerService().registerEventTimeTimer(receipt.eventTime * 1000L + 5000L)
      }
    }

    override def onTimer(timestamp: Long, ctx: CoProcessFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]#OnTimerContext, out: Collector[(OrderEvent, ReceiptEvent)]): Unit = {
      if (payState.value() != null) {
        // receipt 没来，输出 pay 到侧输出流
        ctx.output(unmatchedPays, payState.value())
      }

      if (receiptState.value() != null){
        // pay没来。 输出 receipt 到侧输出流
        ctx.output(unmatchedReceipts, receiptState.value())
      }

      payState.clear()
      receiptState.clear()
    }
  }
}
