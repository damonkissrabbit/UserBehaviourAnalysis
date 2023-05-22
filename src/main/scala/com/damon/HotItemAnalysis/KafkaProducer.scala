package com.damon.HotItemAnalysis

import com.damon.utils.common.producer_prop
import org.apache.kafka.clients.producer.{Callback, KafkaProducer, ProducerRecord, RecordMetadata}

object KafkaProducer {
  def main(args: Array[String]): Unit = {
    writeToKafka("hotItems")
  }

  def writeToKafka(topic: String): Unit = {
    val producer = new KafkaProducer[String, String](producer_prop())

    val bufferedSource = io.Source.fromFile("src/main/resources/HotItemAnalysis/UserBehavior.csv")

    while (true) {
      for (line <- bufferedSource.getLines()) {
        val record = new ProducerRecord[String, String](topic, line)
        producer.send(record, new Callback {
          override def onCompletion(recordMetadata: RecordMetadata, e: Exception): Unit = {
            println("topic: " + recordMetadata.topic() + " partition: " + recordMetadata.partition())
          }
        })
        Thread.sleep(100)
      }
    }

  }
}
