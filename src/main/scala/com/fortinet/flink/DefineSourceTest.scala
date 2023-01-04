package com.fortinet.flink

import com.fortinet.flink.source.ClickSource
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

object DefineSourceTest extends App {

  val env = StreamExecutionEnvironment.getExecutionEnvironment
  val stream = env.addSource(new ClickSource())
   stream.map(new MyRichMapFunction).print() //定义一个richMapFunction
  env.execute()
}
 class MyRichMapFunction extends RichMapFunction[Event,Long] {

   override def open(parameters: Configuration): Unit = {
     println("index number:"+getRuntimeContext.getIndexOfThisSubtask + " task is starting")
   }
   override def close(): Unit = {
     println("index number:"+getRuntimeContext.getIndexOfThisSubtask + " task is ending")
   }
   override def map(value: Event): Long = value.timestamp
 }
