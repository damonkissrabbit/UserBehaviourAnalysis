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


  val prop = new Properties()
  prop.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, zk_servers)
1
  def producer_prop(): Properties = {
    prop.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
    prop.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
    prop
  }

  def consumer_prop(): Properties = {
    prop.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
    prop.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_DOC, classOf[StringDeserializer].getName)
    prop
  }

}
