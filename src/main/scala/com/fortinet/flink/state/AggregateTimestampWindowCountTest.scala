package com.fortinet.flink.state

import com.fortinet.flink.Event
import com.fortinet.flink.source.ClickSource
import org.apache.flink.api.common.functions.{AggregateFunction, RichFlatMapFunction}
import org.apache.flink.api.common.state.{AggregatingState, AggregatingStateDescriptor, ValueStateDescriptor}
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.util.Collector

object AggregateTimestampWindowCountTest extends App {

  val env = StreamExecutionEnvironment.getExecutionEnvironment
  env.setParallelism(1)

  val stream = env.addSource(new ClickSource)
    .assignAscendingTimestamps(_.timestamp)

  stream
    .keyBy(_.user)
    .flatMap(new AvgTimestamp)
    .print("output")

  stream.print("input")
  env.execute()

  class AvgTimestamp extends RichFlatMapFunction[Event, String] {
    lazy val avgTsAggState: AggregatingState[Event, Long] = getRuntimeContext.getAggregatingState(new AggregatingStateDescriptor[Event, (Long, Long), (Long)]("agg-ts", new AggregateFunction[Event, (Long, Long), Long] {
      override def createAccumulator(): (Long, Long) = {
        (0L, 0L)
      }

      override def add(value: Event, accumulator: (Long, Long)): (Long, Long) = {
        (accumulator._1 + value.timestamp, accumulator._2 + 1)
      }

      override def getResult(accumulator: (Long, Long)): Long = {
        accumulator._1 / accumulator._2
      }

      override def merge(a: (Long, Long), b: (Long, Long)): (Long, Long) = ???
    }, classOf[(Long, Long)]))
    lazy val countState = getRuntimeContext.getState(new ValueStateDescriptor[Long]("count", classOf[Long]))

    override def flatMap(value: Event, out: Collector[String]): Unit = {
      avgTsAggState.add(value)
      countState.update(countState.value() + 1)
      if (countState.value() == 5) {
        out.collect(s"${value.user} 的平均时间戳为: ${avgTsAggState.get()}")
        countState.clear()
      }
    }
  }
}
