package com.fortinet.flink.function

import com.fortinet.flink.Event
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

object ProcessWindowFunctionTest {
  def main(args:Array[String]): Unit ={
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    //有序流的水位线，简化设置
//    val stream = env.addSource(new ClickSource()).assignAscendingTimestamps(_.timestamp)
    val stream = env.addSource(new CustomeSource()).assignAscendingTimestamps(_.timestamp)
     stream.keyBy(data=>"true")
//       .process(new KeyedProcessFunction[String,Event,String] {
//         override def processElement(value: Event, ctx: KeyedProcessFunction[String, Event, String]#Context, out: Collector[String]): Unit = {
//           val currentTime = ctx.timerService().currentWatermark() //基于水位线时间
//           out.collect(s"Received data time is $currentTime, current data timestamp ${value.timestamp}")
//           //注册一个5秒之后的定时器
//           ctx.timerService().registerEventTimeTimer(ctx.timestamp() + 5000)
//         }
//         //定义一个定时器处理逻辑
//         override def onTimer(timestamp: Long, ctx: ProcessWindowFunction[String, Event, String]#OnTimerContext, out: Collector[String]): Unit = {
//           out.collect(s"定时触发，触发时间: $timestamp")
//         }
//       })
      .print()
    env.execute()
  }

   class CustomeSource extends SourceFunction[Event] {
     override def run(ctx: SourceFunction.SourceContext[Event]): Unit = {
       ctx.collect(Event("Mary","./home",1000L))
       Thread.sleep(5000)
       ctx.collect(Event("Mary","./home",2000L))
       Thread.sleep(5000)
       ctx.collect(Event("Mary", "./home", 6000L))
       Thread.sleep(5000)
       ctx.collect(Event("Mary", "./home", 6001L))
       Thread.sleep(5000)

     }

     override def cancel(): Unit = ???
   }

}
