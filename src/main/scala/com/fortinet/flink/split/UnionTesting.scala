package com.fortinet.flink.split

import com.fortinet.flink.Event
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.util.Collector

object UnionTesting extends App {
  //定义输出标签

  val env = StreamExecutionEnvironment.getExecutionEnvironment
  env.setParallelism(1)
  val stream1 = env.socketTextStream("localhost",17777)
    .filter(_.split(",").length>2)
    .map(data => {
      println(data)
      val fields = data.split(",")
      Event(fields(0).trim,fields(1).trim,fields(2).trim.toLong)
    }).assignAscendingTimestamps(_.timestamp)

  val stream2 = env.socketTextStream("localhost",18888)
    .filter(_.split(",").length>2)
    .map(data => {
      val fields = data.split(",")
      Event(fields(0).trim, fields(1).trim, fields(2).trim.toLong)
    }).assignAscendingTimestamps(_.timestamp)

  stream1.union(stream2).process(new ProcessFunction[Event,(String,String)] {
    override def processElement(value: Event, ctx: ProcessFunction[Event, (String, String)]#Context, out: Collector[(String, String)]): Unit = {
      out.collect((s"Current Watermark ${ctx.timerService().currentWatermark()}",value.toString))
    }
  }).print()
  env.execute()
}
