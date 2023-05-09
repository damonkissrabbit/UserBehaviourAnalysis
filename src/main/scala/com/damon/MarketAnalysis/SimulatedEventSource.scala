package com.damon.MarketAnalysis

import com.damon.constants.Constants.MarketingUserBehavior
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}

import java.util.UUID
import java.util.concurrent.TimeUnit
import scala.util.Random

class SimulatedEventSource() extends RichSourceFunction[MarketingUserBehavior] {

  var running = true
  val behaviorType: Seq[String] = Seq("Click", "Download", "Install", "Uninstall")
  val channelSets: Seq[String] = Seq("wechat", "facebook", "twitter", "instagram")

  val rand: Random = new Random()

  override def run(ctx: SourceFunction.SourceContext[MarketingUserBehavior]): Unit = {
    val maxElements = Long.MaxValue
    var count = 0L

    while (running && count < maxElements) {
      val id: String = UUID.randomUUID().toString
      val behavior: String = behaviorType(rand.nextInt(behaviorType.size))
      val channel: String = channelSets(rand.nextInt(channelSets.size))
      val ts: Long = System.currentTimeMillis()

      ctx.collect(MarketingUserBehavior(id, behavior, channel, ts))
      count += 1
      TimeUnit.MILLISECONDS.sleep(10L)
    }
  }

  override def cancel(): Unit = {
    running = false
  }
}
