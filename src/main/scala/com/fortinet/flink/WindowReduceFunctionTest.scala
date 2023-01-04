package com.fortinet.flink

import com.fortinet.flink.source.ClickSource
import org.apache.flink.api.common.eventtime._
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time

import java.util.concurrent.TimeUnit

object WindowReduceFunctionTest {
  def main(args:Array[String]): Unit ={
    val env = StreamExecutionEnvironment.getExecutionEnvironment
//    env.getConfig.setAutoWatermarkInterval(500)//Change watermark interval to 500ms
//    env.setParallelism(1)
    val stream = env.addSource(new ClickSource())

    //有序流的水位线
    stream.assignTimestampsAndWatermarks(WatermarkStrategy.forMonotonousTimestamps[Event]()
      .withTimestampAssigner(new SerializableTimestampAssigner[Event]() { //指定时间戳
      override def extractTimestamp(element: Event, recordTimestamp: Long): Long = {
        element.timestamp
      }
    }))
    .map( user =>(user.user,1))
      .keyBy(_._1)
      .window(TumblingEventTimeWindows.of(Time.of(5,TimeUnit.SECONDS)))
      .reduce((state,data)=>(state._1,state._2+data._2))
      .print()

    env.execute()

  }

}

