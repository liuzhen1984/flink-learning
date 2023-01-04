package com.fortinet.flink.tableapi

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.table.functions.AggregateFunction

object UdfTest_AggExample extends App {
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

  //2. 注册聚合函数
  tableEnvironment.createTemporarySystemFunction("weightedAvg",classOf[WeightedAvg])

  //3. 调用函数进行查询转换
  val resultTable = tableEnvironment.sqlQuery("SELECT uid, weightedAvg(ts,1) as avg_ts FROM eventTable GROUP BY uid")

  //4.结果打印输出
  tableEnvironment.toChangelogStream(resultTable).print()

  env.execute()

  case class WeightedAvgAccumulator(var sum:java.lang.Long=0,var count:Int=0)
  class WeightedAvg extends AggregateFunction[java.lang.Long,WeightedAvgAccumulator]{
    override def getValue(acc: WeightedAvgAccumulator): java.lang.Long = {
      if(acc.count==0){
        null
      }else{
        acc.sum/acc.count
      }
    }

    override def createAccumulator(): WeightedAvgAccumulator = {
      WeightedAvgAccumulator()
    }
    def accumulate(acc:WeightedAvgAccumulator,iValue:java.lang.Long,iWeight:Int): Unit ={
      //每一条数据都会调用
      acc.sum += iValue
      acc.count += iWeight
    }
  }

}
