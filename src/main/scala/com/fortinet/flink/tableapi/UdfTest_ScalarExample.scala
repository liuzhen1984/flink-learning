package com.fortinet.flink.tableapi

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.table.functions.ScalarFunction

object UdfTest_ScalarExample extends App {
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

  //2. 注册标量函数
  tableEnvironment.createTemporarySystemFunction("myHash",classOf[MyHash])

  //3. 调用函数进行查询转换
  val resultTable = tableEnvironment.sqlQuery("select uid, myHash(uid) from eventTable")

  //4.结果打印输出
  tableEnvironment.toDataStream(resultTable).print()

  env.execute()

  class MyHash extends ScalarFunction{
    def eval(str:String):Int={
      str.hashCode
    }
  }
}
