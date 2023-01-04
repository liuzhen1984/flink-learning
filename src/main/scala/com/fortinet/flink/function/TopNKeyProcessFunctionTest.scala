package com.fortinet.flink.function

import com.fortinet.flink.UrlValueCount
import com.fortinet.flink.source.ClickSource
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import java.util.concurrent.TimeUnit
import scala.collection.convert.ImplicitConversions.`iterable AsScalaIterable`

object TopNKeyProcessFunctionTest {
  def main(args:Array[String]): Unit ={
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    //有序流的水位线，简化设置
    val stream = env.addSource(new ClickSource()).assignAscendingTimestamps(_.timestamp)
     stream.map(data => data.url.split("\\?")(0))
       .keyBy(data=>data)
       .window(SlidingEventTimeWindows.of(Time.of(10, TimeUnit.SECONDS), Time.of(5, TimeUnit.SECONDS)))
       //UV
       .aggregate(new UrlValueCountAgg, new UrlValueCountResult) //UrlValueCount(./prod,30,1671997048000,1671997058000)
       .keyBy(_.end) //window end time 分组
       .process(new TopN(2)) //实现TopN class, 参数是返回数量
       .print()
    env.execute()
  }
  /*
  Type parameters:
  <K> – Type of the key.
  <I> – Type of the input elements.
  <O> – Type of the output elements.
   */
  class TopN(top:Int) extends KeyedProcessFunction[Long,UrlValueCount,(String,Long)]{
    var urlViewCountListState : ListState[UrlValueCount] = _ //声明
    override def open(parameters: Configuration): Unit = {
      //获取上下文中的state，这里是list state
      //ListStateDescriptor 参数:唯一名称， 保存内容类的声明
       urlViewCountListState = getRuntimeContext.getListState(new ListStateDescriptor[UrlValueCount]("list-state",classOf[UrlValueCount]))
    }

    override def processElement(value: UrlValueCount, ctx: KeyedProcessFunction[Long, UrlValueCount, (String,Long)]#Context, out: Collector[(String,Long)]): Unit = {
      //每个数据处理完要放入listState中
      urlViewCountListState.add(value)
      //定义一个窗口结束时间1ms之后的定时器
      ctx.timerService().registerEventTimeTimer(value.end+1)
    }

    //定时器触发
    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, UrlValueCount, (String, Long)]#OnTimerContext, out: Collector[(String, Long)]): Unit = {
      //处理liststate方法
      urlViewCountListState.get().toList
        .sortBy(-_.count).take(top)
        .foreach(data=>out.collect((data.url,data.count)))
    }
  }

  class UrlValueCountAgg extends AggregateFunction[String, Long, Long] {
    override def createAccumulator(): Long = {
      0
    }

    override def add(value: String, accumulator: Long): Long = {
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
