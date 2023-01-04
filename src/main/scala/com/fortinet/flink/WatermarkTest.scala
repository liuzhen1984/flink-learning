package com.fortinet.flink

import com.fortinet.flink.source.ClickSource
import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.api.common.eventtime._
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.typeutils.TypeSerializer
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.environment
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner
import org.apache.flink.streaming.api.windowing.triggers.Trigger
import org.apache.flink.streaming.api.windowing.windows.Window

import java.time.Duration
import java.util

object WatermarkTest {
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
    //无序流的中位线，
    stream.assignTimestampsAndWatermarks(WatermarkStrategy.forBoundedOutOfOrderness[Event](Duration.ofMillis(500))
      .withTimestampAssigner(new SerializableTimestampAssigner[Event] {
        override def extractTimestamp(element: Event, recordTimestamp: Long): Long = element.timestamp
      })
    )
    //自定义水位线
    stream.assignTimestampsAndWatermarks(new WatermarkStrategy[Event] {

      override def createTimestampAssigner(context: TimestampAssignerSupplier.Context): TimestampAssigner[Event] = {
        new TimestampAssigner[Event] {
          override def extractTimestamp(element: Event, recordTimestamp: Long): Long = element.timestamp
        }
      }

      override def createWatermarkGenerator(context: WatermarkGeneratorSupplier.Context): WatermarkGenerator[Event] = {
        new WatermarkGenerator[Event] {
          //定义一个延迟时间
          val delay = 5000L
          //定义属性保存最大时间戳
          var maxTs =Long.MinValue+delay+1
          override def onEvent(event: Event, eventTimestamp: Long, output: WatermarkOutput): Unit = {
            maxTs = math.max(maxTs,event.timestamp)
          }

          override def onPeriodicEmit(output: WatermarkOutput): Unit = {
            val watermark = new Watermark(maxTs-delay-1)
            output.emitWatermark(watermark)

          }
        }
      }

    })

  }

}

