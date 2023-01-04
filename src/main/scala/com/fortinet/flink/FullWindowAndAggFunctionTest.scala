package com.fortinet.flink

import com.fortinet.flink.source.ClickSource
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import java.util.concurrent.TimeUnit
case class UrlValueCount(url:String, count:Long,start:Long,end:Long){}
object FullWindowAndAggFunctionTest {
  def main(args:Array[String]): Unit ={
    val env = StreamExecutionEnvironment.getExecutionEnvironment
//    env.getConfig.setAutoWatermarkInterval(500)//Change watermark interval to 500ms
//    env.setParallelism(1)
    val stream = env.addSource(new ClickSource())

    //有序流的水位线，简化设置
    stream.assignAscendingTimestamps(_.timestamp)
      .keyBy(_.url)  //以true为key, 只是一个分组
      .window(SlidingEventTimeWindows.of(Time.of(10,TimeUnit.SECONDS),Time.of(2,TimeUnit.SECONDS)))
      //UV
      .aggregate(new UrlValueCountAgg,new UrlValueCountResult)
      //AggregationFunction<IN,ACC,OUT> 输入类型 (IN)、累加器类型(ACC)和输出类型(OUT)
      .print()

    env.execute()
  }

  class UrlValueCountAgg extends AggregateFunction[Event, Long, Long] {
    override def createAccumulator(): Long = {
      0
    }

    override def add(value: Event, accumulator: Long): Long = {
      accumulator + 1 //accumulator + 1
    }

    override def getResult(accumulator: Long): Long = {
      accumulator
    }

    override def merge(a: Long, b: Long): Long = ???
  }

  class UrlValueCountResult extends ProcessWindowFunction[Long, UrlValueCount, String, TimeWindow] {
    override def process(key: String, context: Context, elements: Iterable[Long], out: Collector[UrlValueCount]): Unit = {
      out.collect(UrlValueCount(key, elements.iterator.next(), context.window.getStart, context.window.getEnd))
    }
  }
}
