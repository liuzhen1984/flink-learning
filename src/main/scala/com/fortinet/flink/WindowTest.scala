package com.fortinet.flink

import com.fortinet.flink.source.ClickSource
import org.apache.flink.api.common.eventtime._
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.assigners.{EventTimeSessionWindows, GlobalWindows, SlidingProcessingTimeWindows, TumblingEventTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time

import java.util.concurrent.TimeUnit

object WindowTest {
  def main(args:Array[String]): Unit ={
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.getConfig.setAutoWatermarkInterval(500)//Change watermark interval to 500ms
    val stream = env.addSource(new ClickSource())
    //有序流的水位线
    stream.assignTimestampsAndWatermarks(WatermarkStrategy.forMonotonousTimestamps[Event]()
      .withTimestampAssigner(new SerializableTimestampAssigner[Event]() { //指定时间戳
      override def extractTimestamp(element: Event, recordTimestamp: Long): Long = {
        element.timestamp
      }
    }))
    stream
      .keyBy(_.user)
      .window(TumblingEventTimeWindows.of(Time.of(1,TimeUnit.SECONDS)))
    stream
      .keyBy(_.user)
      .window(EventTimeSessionWindows.withGap(Time.of(1, TimeUnit.SECONDS)))
    stream
      .keyBy(_.user)
      .window(SlidingProcessingTimeWindows.of(Time.of(10, TimeUnit.SECONDS),Time.of(1, TimeUnit.SECONDS)))
    stream
      .keyBy(_.user)
      .countWindow(10,2) //滑动技术窗口
    stream
      .keyBy(_.user)
      .window(GlobalWindows.create())

  }

}

