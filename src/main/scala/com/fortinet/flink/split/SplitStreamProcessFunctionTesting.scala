package com.fortinet.flink.split

import com.fortinet.flink.Event
import com.fortinet.flink.source.ClickSource
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala.{OutputTag, StreamExecutionEnvironment}
import org.apache.flink.util.Collector
object SplitStreamProcessFunctionTesting extends App {
  //定义输出标签
  val maryTag = OutputTag[(String,String,Long)]("mary-tag")
  val bobTag = OutputTag[(String,String,Long)]("bob-tag")
  val env = StreamExecutionEnvironment.getExecutionEnvironment
  env.setParallelism(1)
  val stream = env.addSource(new ClickSource())

  val spStream = stream.process(new ProcessFunction[Event,(String,String)] {
    override def processElement(value: Event, ctx: ProcessFunction[Event, (String, String)]#Context, out: Collector[(String, String)]): Unit = {
      value.user.toLowerCase match {
        case "mary" => ctx.output(maryTag,(value.user,value.url,value.timestamp))
        case "bob" => ctx.output(bobTag,(value.user,value.url,value.timestamp))
        case _ =>out.collect((value.user,value.url))
      }
    }
  })
  //三条输出流
  spStream.print("else")
  spStream.getSideOutput(maryTag).print("mary")
  spStream.getSideOutput(bobTag).print("bob")
  env.execute()

}
