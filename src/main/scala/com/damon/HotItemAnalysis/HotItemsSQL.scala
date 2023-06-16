package com.damon.HotItemAnalysis

import com.damon.utils.common.create_env
import com.damon.constants.Constants.UserBehaviour
import org.apache.flink.types.Row
import org.apache.flink.api.scala._
import org.apache.flink.table.api.{Table, WithOperations}
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.table.api.{EnvironmentSettings, FieldExpression}

object HotItemsSQL {
  def main(args: Array[String]): Unit = {
    val env = create_env()
    val settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build()
    val bEnv = StreamTableEnvironment.create(env, settings)

    env.setParallelism(1)

    val stream = env.readTextFile("src/main/resources/HotItemAnalysis/UserBehavior.csv")
      .map(
        line => {
          val dataArrays = line.split(",")
          UserBehaviour(dataArrays(0).trim.toLong, dataArrays(1).trim.toLong, dataArrays(2).trim.toInt, dataArrays(3), dataArrays(4).toLong * 1000L)
        }
      )
      .filter(data => data.behaviour.equals("pv"))
      .assignAscendingTimestamps(_.timestamp)

    // t：临时视图的名称
    // stream：输入源的名称（假设是一个DataStream）
    // $"itemId"：这是将输入数据源中的 itemId 字段映射为一个 Flink 的 Table 字段，使用 $ 符号可以创建一个字段引用
    // $"timestamp".rowtime() as "ts" 这是将输入数据源中的 "timestamp" 字段映射为 Flink 的 Table 字段，
    // 并将其标记为事件时间（Event Time）。 `rowtime()` 函数用于将字段标记为事件时间， as "ts"  用于为字段指定别名为 ts
    // 使用 $ 的时候要导入 import org.apache.flink.table.api.FieldExpression
    // 使用 rowtime() 需要导入相应的包
    bEnv.createTemporaryView("t", stream, $"itemId", $"timestamp".rowtime() as "ts")

    val result: Table = bEnv.sqlQuery(
      """
        |SELECT
        |	*
        |FROM(
        |	SELECT
        |		*,
        |		ROW_NUMBER() OVER (PARTITION BY windowEnd ORDER BY itemCount DESC) as row_num
        |	FROM (
        |		SELECT
        |			itemId,
        |			COUNT(itemId) as itemCount,
        |			HOP_END(ts, INTERVAL '5' MINUTE, INTERVAL '1' HOUR) as windowEnd
        |		FROM
        |			t
        |		GROUP BY
        |			HOP(ts, INTERVAL '5' MINUTE, INTERVAL '1' HOUR), itemId
        |	)
        |)
        |WHERE
        |	row_num <= 3
        |""".stripMargin
    )

    bEnv.toRetractStream[Row](result).print()
    env.execute()
  }
}
