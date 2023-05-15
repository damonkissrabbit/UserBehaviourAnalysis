package com.damon.NetworkFlowAnalysis

import com.damon.constants.Constants.{UserBehaviour, UvCount}
import com.damon.utils.common.create_env
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import redis.clients.jedis.Jedis

object BloomFilterWithRedis {
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
      .map(data => ("dummyKey", data.userId))
      .keyBy(_._1)
      .timeWindow(Time.minutes(2))
      .trigger(new MyTrigger())
      .process(new BFWithRedis())

    dataStream.print()

    env.execute()

  }
}

class MyTrigger() extends Trigger[(String, Long), TimeWindow] {
  override def onElement(element: (String, Long), timestamp: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = TriggerResult.CONTINUE

  override def onProcessingTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = TriggerResult.CONTINUE

  override def onEventTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = TriggerResult.FIRE_AND_PURGE

  override def clear(window: TimeWindow, ctx: Trigger.TriggerContext): Unit = {}
}


class Bloom(size: Long) extends Serializable {

  private val cap = if (size > 0) size else 1 << 27

  def hash(value: String, seed: Int): Long = {
    var result: Long = 0L
    for (i <- 0 until value.length) {
      result = result * seed + value.charAt(i)
    }
    result & (cap - 1)
  }

}

class BFWithRedis() extends ProcessWindowFunction[(String, Long), UvCount, String, TimeWindow] {

  lazy val jedis = new Jedis("localhost", 6379)

  lazy val bloom = new Bloom(1 << 29)

  override def process(key: String, context: Context, elements: Iterable[(String, Long)], out: Collector[UvCount]): Unit = {
    // 位图的存储方式，key是windowEnd, value是bitmap
    val storeKey = context.window.getEnd.toString
    var count = 0L

    // 把每个窗口的uv-count值也存入count的redis表，存放内容为 (windowEnd -> uvCount)，所以要先从redis里面读取
    if (jedis.hget("count", storeKey) != null) {
      count = jedis.hget("count", storeKey).toLong
    }

    // 用布隆过滤器判断当前用户是否已经存在
    val userId = elements.iterator.next()._2.toString
    val offset: Long = bloom.hash(userId, 61)

    // 定义一个标识位，判断redis位图中有没有这一位
    val isExist = jedis.getbit(storeKey, offset)
    if (!isExist) {
      // 如果不存在，位图对应位置是1，count + 1
      jedis.setbit(storeKey, offset, true)
      jedis.hset("count", storeKey, (count + 1).toString)
      out.collect(UvCount(storeKey.toLong, count + 1))
    } else {
      out.collect(UvCount(storeKey.toLong, count))
    }

  }
}
