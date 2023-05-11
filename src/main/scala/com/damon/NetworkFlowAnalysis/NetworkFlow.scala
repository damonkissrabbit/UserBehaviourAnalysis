package com.damon.NetworkFlowAnalysis

import com.damon.HotItemAnalysis.WindowResult
import com.damon.constants.Constants.{ApacheLogEvent, UrlViewCount}
import com.damon.utils.common.create_env
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.function.{ProcessWindowFunction, WindowFunction}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import java.sql.Timestamp
import java.text.SimpleDateFormat
import scala.collection.mutable.ListBuffer

object NetworkFlow {
  def main(args: Array[String]): Unit = {
    val env = create_env()
    env.setParallelism(10)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val dataStream: DataStream[String] = env.readTextFile("C:\\Users\\star\\Desktop\\UserBehaviourAnalysis\\src\\main\\resources\\NetworkFlowAnalysis\\apache.log")
      .map(data => {
        val dataArrays = data.split(" ")
        val simpleDateFormat = new SimpleDateFormat("dd/MM/yy:HH:mm:ss")
        val timestamp = simpleDateFormat.parse(dataArrays(3).trim).getTime
        ApacheLogEvent(dataArrays(0).trim, dataArrays(1).trim, timestamp, dataArrays(5).trim, dataArrays(6).trim)
      }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[ApacheLogEvent](Time.seconds(5)) {
      override def extractTimestamp(element: ApacheLogEvent): Long = element.eventTime * 1000L
    })
      .keyBy(_.url)
      .timeWindow(Time.seconds(10), Time.seconds(5))
      .allowedLateness(Time.seconds(60))
      .aggregate(new CountAgg(), new WindowResult())
      .keyBy(_.windowEnd)
      .process(new TopNHotUrl(5))

//    dataStream.print()

    env.execute()
  }
}


class CountAgg() extends AggregateFunction[ApacheLogEvent, Long, Long]{
  override def createAccumulator(): Long = 0L

  override def add(value: ApacheLogEvent, accumulator: Long): Long = accumulator + 1

  override def getResult(accumulator: Long): Long = accumulator

  override def merge(a: Long, b: Long): Long = a + b
}


class WindowResult() extends WindowFunction[Long, UrlViewCount, String, TimeWindow]{
  override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[UrlViewCount]): Unit = {
    out.collect(UrlViewCount(key, window.getEnd, input.iterator.next()))
  }
}


class TopNHotUrl(topSize: Int) extends KeyedProcessFunction[Long, UrlViewCount, String]{

  lazy val urlState: ListState[UrlViewCount] = getRuntimeContext.getListState(new ListStateDescriptor[UrlViewCount]("url_state", classOf[UrlViewCount]))

  override def processElement(value: UrlViewCount, ctx: KeyedProcessFunction[Long, UrlViewCount, String]#Context, out: Collector[String]): Unit = {
    // 以 windowEnd 分组， 同一个windowEnd结束的数据都会进入同一个函数
    // 数据进入这里的时候先添加到状态，这里要定义 ListState， 因为数据会有很多
    urlState.add(value)
    ctx.timerService().registerEventTimeTimer(value.windowEnd + 1)
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, UrlViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {
    val allUrlViews = new ListBuffer[UrlViewCount]


//    println(urlState.get())
//    println("******************************************")
    val iter = urlState.get().iterator()
    while (iter.hasNext){
      allUrlViews += iter.next()
    }

    urlState.clear()

    val sortedUrlViews = allUrlViews.sortWith(_.count > _.count)

    val result: StringBuffer = new StringBuffer()
    result.append("time: ").append(new Timestamp(timestamp - 1)).append("\n")

//    for (view <- sortedUrlViews){
//      println(view)
//    }
//    println("*******************************************")
//    sortedUrlViews.foreach(println)
//    println("*******************************************")

//    var i = 0
//    while (i < sortedUrlViews.length){
//      println(sortedUrlViews(i))
//      i += 1
//    }
//    println("*******************************************")

    // 使用迭代器
//    val it = sortedUrlViews.iterator
//    it.foreach(println)
//    println("*******************************************")

    for (i <- sortedUrlViews.indices){
      val currentUrlView = sortedUrlViews(i)
      result.append("No").append(i + 1).append(":")
        .append(" URL = ").append(currentUrlView.url)
        .append(" views = ").append(currentUrlView.count).append("\n")
    }

    result.append("******************************")
    Thread.sleep(1000)
    out.collect(result.toString)
  }
}


class tempProcessFunction() extends ProcessWindowFunction[ApacheLogEvent, String, String, TimeWindow]{
  override def process(key: String, context: Context, elements: Iterable[ApacheLogEvent], out: Collector[String]): Unit = {
    println(context.currentWatermark)
    out.collect(key)
  }
}
