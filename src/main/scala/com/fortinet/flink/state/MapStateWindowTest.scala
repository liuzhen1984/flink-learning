package com.fortinet.flink.state

import com.fortinet.flink.Event
import com.fortinet.flink.source.ClickSource
import org.apache.flink.api.common.state.MapStateDescriptor
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.util.Collector

object MapStateWindowTest extends App {

  val env = StreamExecutionEnvironment.getExecutionEnvironment
  env.setParallelism(1)

  env.addSource(new ClickSource)
    .assignAscendingTimestamps(_.timestamp)
    .keyBy(_.url)
    .process(new FakeWindow(10 * 1000L))
    .print()

  env.execute()

  /**
   * Type parameters:
   * <K> – Type of the key.
   * <I> – Type of the input elements.
   * <O> – Type of the output elements.
   * size = 10秒的滚动窗口
   */
  class FakeWindow(size: Long) extends KeyedProcessFunction[String, Event, (String, Long, Long)] {
    //定义一个映射状态，保存每个窗口的pv值
    lazy val windowPvMapState = getRuntimeContext.getMapState(new MapStateDescriptor[Long, Long]("window-pv", classOf[Long], classOf[Long]))

    override def processElement(value: Event, ctx: KeyedProcessFunction[String, Event, (String, Long, Long)]#Context, out: Collector[(String, Long, Long)]): Unit = {
      //当前数据落入的起始时间戳
      val start = value.timestamp / size * size //整数除法得到起始点
      val end = start + size //结束点

      ctx.timerService().registerEventTimeTimer(end - 1)
      if (windowPvMapState.contains(start)) {
        windowPvMapState.put(start, windowPvMapState.get(start) + 1)
      } else {
        windowPvMapState.put(start, 1)
      }
    }

    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, Event, (String, Long, Long)]#OnTimerContext, out: Collector[(String, Long, Long)]): Unit = {
      val start = timestamp + 1 - size
      val pv = windowPvMapState.get(start)
      out.collect((ctx.getCurrentKey, pv, start))
      windowPvMapState.remove(start)
    }
  }
}
