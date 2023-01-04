package com.fortinet.flink.function

import com.fortinet.flink.source.ClickSource
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.function.ProcessAllWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object TopNProcessAllWindowFunctionTest {
  def main(args:Array[String]): Unit ={
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    //有序流的水位线，简化设置
    val stream = env.addSource(new ClickSource()).assignAscendingTimestamps(_.timestamp)
     stream.map(data => data.url.split("\\?")(0))
       .windowAll(SlidingEventTimeWindows.of(Time.seconds(10),Time.seconds(5)))
       /*
       Type parameters:
       <IN> – The type of the input value.
       <OUT> – The type of the output value.
       <W> – The type of Window that this window function can be applied on.
        */
       .process(new ProcessAllWindowFunction[String,(String,Long),TimeWindow] {
         override def process(context: Context, elements: Iterable[String], out: Collector[(String, Long)]): Unit = {
            elements.groupBy(t=>t).toList.sortBy(-_._2.size).take(2).foreach(
              data=>out.collect((data._1,data._2.size))
            )
         }
       }).print()
    env.execute()
  }
}
