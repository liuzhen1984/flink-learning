package com.fortinet.flink.tableapi

import org.apache.flink.table.api.Expressions.$
import org.apache.flink.table.api.{EnvironmentSettings, TableEnvironment}

object CommonTableAPITest {

  def main(args: Array[String]): Unit = {
    //    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //    env.setParallelism(1)
    //    val tableEnv = StreamTableEnvironment.create(env)

    //可省略StreamExecutionEnvironment to get ExecutionEnvironment
    val tableEnvironment = TableEnvironment.create(EnvironmentSettings.newInstance().inBatchMode().build())

    //创建表
    tableEnvironment.executeSql("CREATE TABLE eventTable (" +
      " uid STRING," +
      "url STRING" +
      "ts BIGINT" +
      ") WITH (" +
      " 'connector' = 'filesystem'," +
      " 'path'='input/clicks.txt'," +
      " 'format'='csv'," +
      ")")
    //表查询
    val resultTable = tableEnvironment.sqlQuery("select uid,url,ts from eventTable where uid = 'Alice")
    //每个用户的用户访问频次
    val aggResult = tableEnvironment.sqlQuery("select uid,count(url) from eventTable group by uid")

    //table API
    val eventTable = tableEnvironment.from("eventTable")
    val tableResult = eventTable.where($("url").isEqual("./home"))
      .select($("url"),$("uid"),$("ts"))

//    输出表的创建
    tableEnvironment.executeSql("CREATE TABLE outputTable (" +
      " uid STRING," +
      "url STRING" +
      "ts BIGINT" +
      ") WITH (" +
      " 'connector' = 'filesystem'," +
      " 'path'='output'," +
      " 'format'='csv'," +
      ")")

    //将结果写入输出表
    resultTable.executeInsert("outputTable")
    //    env.execute()
  }
}
