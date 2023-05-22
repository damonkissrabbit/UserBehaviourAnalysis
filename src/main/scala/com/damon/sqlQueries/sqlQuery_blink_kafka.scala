package com.damon.sqlQueries

import com.damon.constants.Constants.{UserBehaviour, kafka_servers}
import com.damon.utils.common.create_env
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer

import java.util.Properties


object sqlQuery_blink_kafka {
  def main(args: Array[String]): Unit = {
    val env = create_env()
    env.setParallelism(10)
    val settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build()
    val bsEnv = StreamTableEnvironment.create(env, settings)

    val prop = new Properties()
    prop.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka_servers)
    prop.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
    prop.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
    prop.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, "org.apache.kafka.clients.consumer.RoundRobinAssignor")
    prop.put(ConsumerConfig.GROUP_ID_CONFIG, "hotItemGroup")
    prop.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest")

    val kafkaStream = new FlinkKafkaConsumer[String]("hotItems", new SimpleStringSchema(), prop)

    val dataStream = env.addSource(kafkaStream)
      .map(data => {
        val dataArrays = data.split(",")
        UserBehaviour(dataArrays(0).trim.toLong, dataArrays(1).trim.toLong, dataArrays(2).trim.toInt, dataArrays(3).trim, dataArrays(4).trim.toLong)
      })

    val table = bsEnv.fromDataStream(dataStream)

    bsEnv.createTemporaryView("userBehavior", table)

    val result = bsEnv.sqlQuery(
      """
        |select * from userBehavior
        |""".stripMargin
    )

    result.execute().print()

    env.execute()
  }
}
