package com.fortinet.flink.tableapi

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.Expressions.{$, call}
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.table.functions.TableAggregateFunction
import org.apache.flink.util.Collector

import java.sql.Timestamp

object UdfTest_TableAggExample extends App {
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
  tableEnvironment.createTemporarySystemFunction("top2", classOf[TopN])

  //3. 调用函数进行查询转换

  //首先进行窗口聚合得到cnt的值
  val urlCountWindowTable = tableEnvironment.sqlQuery(
    """
      |SELECT uid, COUNT(url) AS cnt, window_start as wstart,window_end as wend
      |FROM TABLE(
      |   TUMBLE(TABLE eventTable, DESCRIPTOR(et), INTERVAL '1' HOUR)
      |)
      |GROUP BY uid,window_start,window_end
      |""".stripMargin)
  tableEnvironment.createTemporaryView("urlCountWindowTable", urlCountWindowTable)

  //使用table API调用表聚合函数
  val resultTable = urlCountWindowTable.groupBy($("wend"))
    .flatAggregate(call("top2",$("uid"),$("cnt"),$("wstart"),$("wend")))
    .select($("uid"),$("rank"),$("cnt"),$("wend"))


  //4.结果打印输出
  tableEnvironment.toChangelogStream(resultTable).print()

  env.execute()
  case class Top2Result(uid:String, window_start:Timestamp, window_end:Timestamp, cnt:Long, rank:Int)
  case class Top2Accumulator(var maxCount:Long,var secondMaxCount:Long,var uid1:String,var uid2:String,var window_start:Timestamp,var window_end:Timestamp)

  class TopN extends TableAggregateFunction[Top2Result,Top2Accumulator] {
    override def createAccumulator(): Top2Accumulator = {
      Top2Accumulator(Long.MinValue,Long.MinValue,null,null,null,null)
    }
    //实现accumulate方法
    def accumulate(acc:Top2Accumulator, uid:String, cnt:Long, window_start:Timestamp, window_end:Timestamp): Unit ={
      acc.window_start = window_start
      acc.window_end = window_end
      //判断当前count值是否排名前两位
      if(cnt>acc.maxCount){
        //名词向后顺延
        acc.secondMaxCount = acc.maxCount
        acc.uid2 = acc.uid1
        acc.maxCount = cnt
        acc.uid1 = uid
      }else if(cnt>acc.secondMaxCount){
        acc.secondMaxCount = cnt
        acc.uid2 = uid
      }
    }

    //输出结果数据
    def emitValue(acc:Top2Accumulator,out:Collector[Top2Result]): Unit ={
      //判断cnt值是否为初始值，如果没有更新过，直接跳过不输出
      if(acc.maxCount!=Long.MinValue){
        out.collect(Top2Result(acc.uid1,acc.window_start,acc.window_end,acc.maxCount,1))
      }
      if (acc.secondMaxCount!=Long.MinValue){
        out.collect(Top2Result(acc.uid2,acc.window_start,acc.window_end,acc.secondMaxCount,2))
      }
    }
  }
}
