package com.damon.sqlQueries

import org.apache.flink.types.Row
import com.damon.utils.common.create_env
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.table.api.{EnvironmentSettings, FieldExpression, Table}
import org.apache.flink.table.api.bridge.scala.{StreamTableEnvironment, tableConversions}



object blink {
  def main(args: Array[String]): Unit = {
    val bsEnv = create_env()
    val bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build()
    // StreamTableEnvironment 是 Flink中用于执行和管理表操作的主要入口点，它可以使用Table Api和 Sql对数据进行转换，查询和写入
    val bsTableEnv = StreamTableEnvironment.create(bsEnv, bsSettings)

    val sourceStream: DataStream[String] = bsEnv.socketTextStream("192.168.0.112", 8899)

    val table: Table = bsTableEnv.fromDataStream(sourceStream, $"word")

    bsTableEnv.createTemporaryView("words", table)

    val countTable: Table = bsTableEnv.sqlQuery(
      """
        |select word, count(1) from words group by word
        |""".stripMargin
    )

    val result: DataStream[(Boolean, Row)] = countTable.toRetractStream[Row]

    result.print()
    bsEnv.execute()
  }
}
