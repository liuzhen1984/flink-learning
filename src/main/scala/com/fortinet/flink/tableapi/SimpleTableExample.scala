package com.fortinet.flink.tableapi

import com.fortinet.flink.source.ClickSource
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.Expressions.$
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment

object SimpleTableExample {

  def main(args:Array[String]): Unit ={
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val eventStream = env.addSource(new ClickSource)
    //创建表环境
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env)
    //将datastream 转为table
    val table = tableEnv.fromDataStream(eventStream)

    //调用tableAPI 进行转换
    val result = table.select($("user"),$("url")).where($("user").isEqual("Alice"))

    //直接sql
    val resultSQL = tableEnv.sqlQuery("select url,user from "+table+" where user='Bob'")
    //转换成datastream 输出
    tableEnv.toDataStream(result).print("select:")
    tableEnv.toDataStream(resultSQL).print("sql:")
    env.execute()
  }
}
