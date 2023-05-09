package com.damon.constants

import com.esotericsoftware.kryo.serializers.DefaultSerializers.StringSerializer
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.StringDeserializer

import java.util.Properties

object Constants {
  val zk_servers = "master:9002, slave1:9002, slave2:9002"

  case class UserBehaviour(userId: Long, itemId: Long, categoryId: Int, behaviour: String, timestamp: Long)

  case class ItemViewCount(itemId: Long, windowEnd: Long, count: Long)

  case class LoginEvent(userId: Long, ip: String, eventType: String, eventTime: Long)

  case class Warning(userId: Long, firstFailTime: Long, lastFailTime: Long, warningMsg: String)

  case class MarketingUserBehavior(userId: String, behavior: String, channel: String, timestamp: Long)


}
