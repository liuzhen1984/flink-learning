package com.fortinet.flink

import com.fortinet.flink.source.ClickSource
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import java.time.Duration
import java.util.concurrent.TimeUnit
object WatermarkAndFunctionTest {
  def main(args:Array[String]): Unit ={
    val env = StreamExecutionEnvironment.getExecutionEnvironment
//    env.getConfig.setAutoWatermarkInterval(500)//Change watermark interval to 500ms
//    env.setParallelism(1)
    val stream = env.addSource(new ClickSource())

    //有序流的水位线，简化设置
    stream.assignTimestampsAndWatermarks(WatermarkStrategy.forBoundedOutOfOrderness[Event](Duration.ofSeconds(5)).withTimestampAssigner(
      new SerializableTimestampAssigner[Event] {
        override def extractTimestamp(element: Event, recordTimestamp: Long): Long = element.timestamp
      }
    ))
      .keyBy(_.user)  //user分组
      //用户窗口内活跃度统计
      .window(TumblingEventTimeWindows.of(Time.of(10,TimeUnit.SECONDS)))
      //UV
      .process(new ProcessWindowFunction[Event,String,String,TimeWindow] {
        override def process(key: String, context: Context, elements: Iterable[Event], out: Collector[String]): Unit = {
          //提取信息
          val start = context.window.getStart
          val end = context.window.getEnd
          val count = elements.size
          //增加水位线信息
          val currentWatermark = context.currentWatermark

          out.collect(s"the window $start-$end, user $key, count=$count, watermark=$currentWatermark")
        }
      })
      .print()

    env.execute()
  }
}
