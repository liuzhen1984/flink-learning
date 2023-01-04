package com.fortinet.flink.function

import com.fortinet.flink.Event
import com.fortinet.flink.source.ClickSource
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.util.Collector
object ProcessFunctionTest {
  def main(args:Array[String]): Unit ={
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val stream = env.addSource(new ClickSource())

    //有序流的水位线，简化设置
    stream.assignAscendingTimestamps(_.timestamp)
      .process(new ProcessFunction[Event,String] {
        override def processElement(value: Event, ctx: ProcessFunction[Event, String]#Context, out: Collector[String]): Unit = {
          out.collect(value.user)
          out.collect(value.url)
          println(getRuntimeContext.getIndexOfThisSubtask)
          println(ctx.timerService().currentWatermark())
        }
      })
      .print()

    env.execute()
  }


}
