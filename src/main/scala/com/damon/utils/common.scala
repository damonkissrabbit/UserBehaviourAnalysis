package com.damon.utils

import com.damon.constants.Constants.kafka_servers
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.StringDeserializer

import java.util.Properties

object common {
  val prop = new Properties()

  def producer_prop(): Properties = {
    prop.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka_servers)
    prop.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
    prop.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
    prop
  }

  def consumer_prop(): Properties = {
    prop.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka_servers)
    prop.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
    prop.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_DOC, classOf[StringDeserializer].getName)
    prop
  }

  def create_env(): StreamExecutionEnvironment = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env
  }

}
