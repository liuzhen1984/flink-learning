package com.fortinet.flink.tableapi

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment

object TopNExample extends App {
  val env = StreamExecutionEnvironment.getExecutionEnvironment

  env.setParallelism(1)
  val tableEnvironment = StreamTableEnvironment.create(env)

  //创建表
  tableEnvironment.executeSql("CREATE TABLE eventTable (" +
    " uid STRING," +
    " url STRING," +
    " ts BIGINT," +
    " et AS TO_TIMESTAMP( FROM_UNIXTIME(ts/1000) )," +
    " WATERMARK FOR et AS et - INTERVAL '2' SECOND" +
    ") WITH (" +
    " 'connector' = 'filesystem'," +
    " 'path'='input/clicks.txt'," +
    " 'format' = 'csv'" +
    ")")

  //TOP N 获取活跃度最大的两个用户
  //1. 进行分组聚合统计，计算每个用户点击的数量
  val urlCountTable = tableEnvironment.sqlQuery("select uid, count(url) as cnt from eventTable group by uid")
  tableEnvironment.createTemporaryView("urlCountTable", urlCountTable)

  //2. 提取count值 最大的前两个用户
  val top2Result = tableEnvironment.sqlQuery(
    """
      | SELECT uid, cnt, row_num
      | FROM (
      |    SELECT *,ROW_NUMBER() OVER (
      |       ORDER BY cnt DESC
      |    ) as row_num
      |    FROM urlCountTable
      | )
      | WHERE row_num <= 2
      |""".stripMargin)
  //因为需要更新表，所以需要使用changelogStream
  tableEnvironment.toChangelogStream(top2Result).print()
  env.execute()

}
