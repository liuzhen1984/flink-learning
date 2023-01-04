package com.fortinet.flink.tableapi

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.annotation.{DataTypeHint, FunctionHint}
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.table.functions.TableFunction
import org.apache.flink.types.Row

object UdfTest_TableExample extends App {
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

  //2. 注册表函数
  tableEnvironment.createTemporarySystemFunction("mySplit",classOf[SplitUrl])

  //3. 调用函数进行查询转换
  val resultTable = tableEnvironment.sqlQuery("SELECT uid, url,word, len FROM eventTable, LATERAL TABLE(mySplit(url)) AS T(word,len)")

  //4.结果打印输出
  tableEnvironment.toDataStream(resultTable).print()

  env.execute()

  @FunctionHint(output=new DataTypeHint("ROW<word String, len INT>"))
  class SplitUrl extends TableFunction[Row]{
    def eval(str: String): Unit = {
      str.split("\\?").foreach(s=>collect(Row.of(s,Int.box(s.length))))
    }
  }
}
