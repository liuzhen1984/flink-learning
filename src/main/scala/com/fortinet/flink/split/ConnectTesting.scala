package com.fortinet.flink.split

import com.fortinet.flink.Event
import com.fortinet.flink.source.ClickSource
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.functions.co.CoMapFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

object ConnectTesting extends App {
  //定义输出标签

  val env = StreamExecutionEnvironment.getExecutionEnvironment
  env.setParallelism(1)
  val stream1 = env.addSource(new ClickSource())

  val stream2 = env.fromElements(1L,2L,3L)

  //
  stream1.connect(stream2) //返回ConnectedStreams

    /**
     * Type parameters:
     * <IN1> – Type of the first input.
     * <IN2> – Type of the second input.
     * <OUT> – Output type.
     */
    .map(new CoMapFunction[Event,Long,(String,Long)] {
      override def map1(value: Event): (String, Long) = {
        (value.user,transToLong(value.user))
      }
      override def map2(value: Long): (String, Long) = {
        (transToName(value),value)
      }
    })
    .print()

  env.execute()
  def transToLong(name:String):Long={
    name.trim.toLowerCase() match {
      case "Mary" => 1
      case "bob" => 2
      case _ => 3
    }
  }

  def transToName(code: Long): String = {
    code match {
      case 1 => "Mary"
      case 2 => "Bob"
      case 3 => "Other"
    }
  }
}
