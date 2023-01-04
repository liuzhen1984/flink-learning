package com.fortinet.flink.state

import com.fortinet.flink.Event
import com.fortinet.flink.source.ClickSource
import org.apache.flink.api.common.functions.{AggregateFunction, ReduceFunction, RichFlatMapFunction}
import org.apache.flink.api.common.state.{AggregatingState, AggregatingStateDescriptor, ReducingState, ReducingStateDescriptor}
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.util.Collector

object KeyedStateTest extends App {

  val env = StreamExecutionEnvironment.getExecutionEnvironment
  env.setParallelism(1)

  env.addSource(new ClickSource)
    .assignAscendingTimestamps(_.timestamp)
    .keyBy(_.user)
    .flatMap(new MyMap)
    .print()

  env.execute()

  //Type parameters:
  //<IN> – Type of the input elements.
  // <OUT> – Type of the returned elements.
  class MyMap extends RichFlatMapFunction[Event,String] {
    //定义一个值状态
    var reduceState: ReducingState[Event] = _
    var aggState: AggregatingState[Event,String] = _

    //需要在第一次启动时候赋值初始化
    override def open(parameters: Configuration): Unit = {
      reduceState = getRuntimeContext.getReducingState(new ReducingStateDescriptor[Event]("reduce-state", new ReduceFunction[Event] {
        override def reduce(value1: Event, value2: Event): Event = {
          Event(value1.user,value1.url,value2.timestamp)
        }
      },classOf[Event]))

      /**
       *  Type parameters:
       *  <IN> – The type of the values that are added to the state.
       *  <ACC> – The type of the accumulator (intermediate aggregation state).
       *  <OUT> – The type of the values that are returned from the state.
       */
      aggState = getRuntimeContext.getAggregatingState(new AggregatingStateDescriptor[Event, Long,String]("my-agg",new AggregateFunction[Event,Long,String] {
        override def createAccumulator(): Long = 0L

        override def add(value: Event, accumulator: Long): Long = accumulator+1

        override def getResult(accumulator: Long): String = "聚合状态内的值 :"+accumulator.toString

        override def merge(a: Long, b: Long): Long = ???
      },classOf[Long]))
    }

    override def flatMap(value: Event, out: Collector[String]): Unit = {
      reduceState.add(value)
      println(reduceState.get())
      aggState.add(value)
      println(aggState.get())
      println("++++++++++++++++")
    }
  }
}
