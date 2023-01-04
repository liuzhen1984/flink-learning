package com.fortinet.flink.function

import com.fortinet.flink.Event
import com.fortinet.flink.source.ClickSource
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.util.Collector
object KeyedProcessTimerFunctionTest {
  def main(args:Array[String]): Unit ={
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    //有序流的水位线，简化设置
    val stream = env.addSource(new ClickSource()).assignAscendingTimestamps(_.timestamp)
     stream.keyBy(data=>"true")
       .process(new KeyedProcessFunction[String,Event,String] {
         override def processElement(value: Event, ctx: KeyedProcessFunction[String, Event, String]#Context, out: Collector[String]): Unit = {
           val currentTime = ctx.timerService().currentProcessingTime()
           out.collect(s"Received data time is $currentTime")
           //注册一个5秒之后的定时器
           ctx.timerService().registerProcessingTimeTimer(currentTime + 5000)
         }
         //定义一个定时器处理逻辑
         override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, Event, String]#OnTimerContext, out: Collector[String]): Unit = {
           out.collect(s"定时触发，触发时间: $timestamp")
         }
       })
      .print()
    env.execute()
  }


}
