package com.fortinet.flink

import com.fortinet.flink.source.ClickSource
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import java.util.concurrent.TimeUnit

object FullWindowFunctionTest {
  def main(args:Array[String]): Unit ={
    val env = StreamExecutionEnvironment.getExecutionEnvironment
//    env.getConfig.setAutoWatermarkInterval(500)//Change watermark interval to 500ms
//    env.setParallelism(1)
    val stream = env.addSource(new ClickSource())

    //有序流的水位线，简化设置
    stream.assignAscendingTimestamps(_.timestamp)
    .map( user =>(user.user,1))
      .keyBy(_=>"true")  //以true为key, 只是一个分组
      .window(TumblingEventTimeWindows.of(Time.of(10,TimeUnit.SECONDS)))
      //UV
      .process(new UvWindow())
      //AggregationFunction<IN,ACC,OUT> 输入类型 (IN)、累加器类型(ACC)和输出类型(OUT)
      .print()

    env.execute()
  }
}

class UvWindow extends ProcessWindowFunction[(String, Int), (String, Long), String, TimeWindow] {
  override def process(key: String, context: Context, elements: Iterable[(String, Int)], out: Collector[(String, Long)]): Unit = {
    //使用set 去重操作
    var userSet = Set[String]()
    //从elements中获取数据去重
    elements.foreach(userSet += _._1)
    val uv = userSet.size
    val windowEnd = context.window.getEnd
    val windowStart = context.window.getStart
    out.collect((s"${windowStart}-${windowEnd} window", uv))
  }
}