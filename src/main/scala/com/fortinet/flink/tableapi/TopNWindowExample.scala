package com.fortinet.flink.tableapi

import com.fortinet.flink.Event
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.Expressions.$
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment

import java.time.Duration

object TopNWindowExample extends App {
  val env = StreamExecutionEnvironment.getExecutionEnvironment

  env.setParallelism(1)
  val tableEnvironment = StreamTableEnvironment.create(env)

  //取代创建表过程
  val eventStream = env.fromElements(
    Event("Alice", "./home", 1000L),
    Event("Bob", "./cart", 1000L),
    Event("Alice", "./prod?id=1", 25 * 60 * 1000L),
    Event("Alice", "./prod?id=3", 55 * 60 * 1000L),
    Event("Bob", "./prod?id=5", 3600 * 1000L + 60 * 1000L),
    Event("Cary", "./home", 3600 * 1000L + 30 * 60 * 1000L),
    Event("Cary", "./prod?id=7", 3600 * 1000L + 59 * 60 * 1000L),
  ).assignTimestampsAndWatermarks(WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(2))
    .withTimestampAssigner(new SerializableTimestampAssigner[Event] {
      override def extractTimestamp(element: Event, recordTimestamp: Long): Long = {
        element.timestamp
      }
    })
  )

  //将DataStream转换成表
  val eventTable = tableEnvironment.fromDataStream(eventStream,$("url"),$("user").as("uid"),$("timestamp").as("ts"),$("et").rowtime())

  tableEnvironment.createTemporaryView("eventTable",eventTable)
  //窗口TOP N 选取每小时内的获取度最大的前两个用户
  //1. 进行分组聚合统计，计算每个用户点击的数量

  val urlCountWindowTable = tableEnvironment.sqlQuery(
    """
      |SELECT uid, COUNT(url) AS cnt, window_start,window_end
      |FROM TABLE(
      |   TUMBLE(TABLE eventTable, DESCRIPTOR(et), INTERVAL '1' HOUR)
      |)
      |GROUP BY uid,window_start,window_end
      |""".stripMargin)
  tableEnvironment.createTemporaryView("urlCountWindowTable", urlCountWindowTable)

  //2. 提取count值 最大的前两个用户
  val top2Result = tableEnvironment.sqlQuery(
    """
      | SELECT *
      | FROM (
      |    SELECT *,ROW_NUMBER() OVER (
      |       PARTITION BY window_start,window_end
      |       ORDER BY cnt DESC
      |    ) as row_num
      |    FROM urlCountWindowTable
      | )
      | WHERE row_num <= 2
      |""".stripMargin)
  //因为需要更新表，所以需要使用changelogStream
  tableEnvironment.toDataStream(top2Result).print()
  env.execute()

}
