package com.fortinet.flink

import com.fortinet.flink.source.ClickSource
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time

import java.util.concurrent.TimeUnit

object WindowAggregationFunctionTest {
  def main(args:Array[String]): Unit ={
    val env = StreamExecutionEnvironment.getExecutionEnvironment
//    env.getConfig.setAutoWatermarkInterval(500)//Change watermark interval to 500ms
//    env.setParallelism(1)
    val stream = env.addSource(new ClickSource())

    //有序流的水位线，简化设置
    stream.assignAscendingTimestamps(_.timestamp)
      .keyBy(_=>"true")  //以true为key, 只是一个分组
      .window(SlidingEventTimeWindows.of(Time.of(10,TimeUnit.SECONDS),Time.seconds(2)))
      //统计PV和UV的值，得到PV/UV
      .aggregate(new PvUv)
      //AggregationFunction<IN,ACC,OUT> 输入类型 (IN)、累加器类型(ACC)和输出类型(OUT)
      .print()

    env.execute()

  }

  class PvUv extends AggregateFunction[Event, (Long,List[String]), Double] {
    //初始化调用一次的方法,设置初始化的值

    override def createAccumulator(): (Long, List[String]) = {
      (0,Nil)
    }

    override def add(value: Event, accumulator: (Long, List[String])): (Long, List[String]) = {
      if(accumulator._2.contains(value.user)){
        (accumulator._1+1,accumulator._2)
      } else{
        (accumulator._1+1,accumulator._2.+:(value.user))
      }
    }

    override def getResult(accumulator: (Long, List[String])): Double = ???

    override def merge(a: (Long, List[String]), b: (Long, List[String])): (Long, List[String]) = ???
  }


}
