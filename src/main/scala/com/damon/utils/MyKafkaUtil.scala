package com.damon.utils

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, FlinkKafkaProducer}

import java.util.Properties
import com.damon.constants.Constants.zk_servers
import com.damon.utils.common.producer_prop
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer

object MyKafkaUtil {
  val prop: Properties = producer_prop()
  prop.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
  prop.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_DOC, classOf[StringDeserializer].getName)
  prop.setProperty("group.id", "consumer_group")
  prop.setProperty("auto.offset.reset", "latest")

  def getConsumer(topic: String): FlinkKafkaConsumer[String] = {
    val myKafkaConsumer = new FlinkKafkaConsumer[String](topic, new SimpleStringSchema(), prop)
    myKafkaConsumer
  }

  def getProducer(topic: String): FlinkKafkaProducer[String] = {
    new FlinkKafkaProducer[String](zk_servers, topic, new SimpleStringSchema())
  }


}
